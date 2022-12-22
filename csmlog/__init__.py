"""
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2021) - Charles Machalow
"""

import logging
import logging.handlers
import os
import shutil
import sys
import uuid
from pathlib import Path

from csmlog.google_sheets_handler import GSheetsHandler
from csmlog.system_call import LoggedSystemCall
from csmlog.udp_handler import UdpHandler
from csmlog.udp_handler_receiver import UdpHandlerReceiver

__version__ = "0.26.0"

DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s"


class CSMLogger(object):
    """
    object to wrap logging logic
    """

    def __init__(
        self,
        appName,
        clearLogs=False,
        udpLogging=True,
        googleSheetShareEmail=None,
        formatter=None,
        modifyChildLoggersFunc=None,
    ):
        self.appName = appName
        self.udpLogging = udpLogging
        self.googleSheetShareEmail = googleSheetShareEmail

        # A function to call when creating a child logger. This is useful for adding in
        # things like handlers to all child loggers.
        self.modifyChildLoggersFunc = modifyChildLoggersFunc

        self._loggers = []

        # sets self._formatter
        self.setFormatter(formatter)

        if clearLogs:
            self.clearLogs()

        self.parentLogger = self.__getParentLogger()
        self.consoleLoggingStream = None
        self._loggers = [self.parentLogger]  # keep track of all loggers

    def close(self):
        for logger in self._loggers:
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)

        self._loggers = []

    def getLogger(self, name):
        name = os.path.basename(name)
        loggerName = "%s.%s" % (
            self.appName,
            name,
        )  # make this a sublogger of the whole app
        logger = self.__getLoggerWithName(loggerName)
        self._loggers.append(logger)
        logger.sysCall = LoggedSystemCall(logger)

        if self.modifyChildLoggersFunc:
            logger = self.modifyChildLoggersFunc(logger)

        return logger

    def __getParentLogger(self):
        logger = self.__getLoggerWithName(self.appName)
        if self.udpLogging:
            handler = UdpHandler()
            handler.setFormatter(self.getFormatter())
            logger.addHandler(handler)

        if self.googleSheetShareEmail:
            handler = GSheetsHandler(self.appName, self.googleSheetShareEmail)
            handler.setFormatter(self.getFormatter())
            logger.addHandler(handler)

        return logger

    def __getLoggerWithName(self, loggerName):
        logger = logging.getLogger(loggerName)
        logger.setLevel(1)  # log all

        logFolder = self.getDefaultSaveDirectory()

        logFile = os.path.join(logFolder, loggerName + ".txt")

        formatter = self.getFormatter()

        class RotatingFileHandlerThatWillKeepWorkingOnPermissionErrorDuringRotate(
            logging.handlers.RotatingFileHandler
        ):
            """
            This class is special, it exists because on Windows, file names can't be changed while files are open.
                So ultimately we can't rotate if 2 processes are writing to a given log file.
                The default RotatingFileHandler will just throw the PermissionError and stop writing
                        (since it closed the stream already)
                By throwing away the PermissionError (since logging it on every log statement would be too much),
                    and using the delay=True parameter to RotatingFileHandler, we will just continue writing to the
                    original file without rotating, via reopening the original file. If the other process closes the file,
                    it will rotate on the next log statement
            """

            # todo: probably write test cases for rotation with multiple processes writing to one log file.
            #   ensure some sort of semi-deterministic behavior
            def rotate(self, source, dest):
                try:
                    logging.handlers.RotatingFileHandler.rotate(self, source, dest)
                except PermissionError:
                    pass

        rfh = RotatingFileHandlerThatWillKeepWorkingOnPermissionErrorDuringRotate(
            logFile, maxBytes=1024 * 1024 * 8, backupCount=10, delay=True
        )
        rfh.setFormatter(formatter)
        logger.addHandler(rfh)

        # add the log file path / folder for easy access elsewhere
        logger.logFile = logFile
        logger.logFolder = logFolder
        logger.loggerName = loggerName

        return logger

    def getFormatter(self):
        return self._formatter

    def getDefaultSaveDirectory(self):
        return self.getDefaultSaveDirectoryWithName(self.appName)

    def enableConsoleLogging(self, level=1, stream=None):
        if stream is None:
            # evaluate sys.stderr later since pytest may change it
            stream = sys.stderr

        if self.consoleLoggingStream:
            self.disableConsoleLogging()

            # recursive
            return self.enableConsoleLogging(level=level, stream=stream)
        else:
            self.consoleLoggingStream = logging.StreamHandler(stream)
            self.consoleLoggingStream.setFormatter(self.getFormatter())
            self.parentLogger.addHandler(self.consoleLoggingStream)

        self.consoleLoggingStream.setLevel(level)

    def disableConsoleLogging(self):
        if not self.consoleLoggingStream:
            raise RuntimeError("Managed console logging is not active")

        self.parentLogger.removeHandler(self.consoleLoggingStream)
        self.consoleLoggingStream = None

    def setFormatter(self, formatter=None):
        if formatter is None:
            formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

        if isinstance(formatter, str):
            formatter = logging.Formatter(formatter)

        self._formatter = formatter

        for logger in self._loggers:
            for handler in logger.handlers:
                handler.setFormatter(formatter)

    @classmethod
    def getDefaultSaveDirectoryWithName(cls, appName):
        if os.name == "nt":
            logFolder = os.path.join(os.path.expandvars("%APPDATA%"), appName)
        else:
            tmpPath = Path(f"/var/log/{uuid.uuid4()}")
            try:
                tmpPath.touch()
                tmpPath.unlink()
                tmpPath = tmpPath.parent
            except PermissionError:
                # can't use /var/log... try using ~/log/
                tmpPath = Path.home() / "log"
                tmpPath.mkdir(exist_ok=True)

            logFolder = tmpPath / appName

        if not os.path.isdir(logFolder):
            os.makedirs(logFolder)

        return logFolder

    def clearLogs(self):
        shutil.rmtree(self.getDefaultSaveDirectory())

        # recreate empty folder
        self.getDefaultSaveDirectory()


class _CSMLoggerManager:
    """manages the active instance (and older instances) of CSMLogger"""

    def __init__(self):
        # loggers that are no longer default (setup() was called again, though may still be in use)
        self._oldCsmLoggers = []

        # The currently active logger
        self._activeCsmLogger = None

        # publish methods from this guy
        for name in dir(self):
            if name.startswith("_") or name.endswith("_"):
                continue

            globals()[name] = getattr(self, name)

    def getLogger(self, *args, **kwargs):
        if not self._activeCsmLogger:
            raise RuntimeError("(csmlog) setup() must be called first!")

        return self._activeCsmLogger.getLogger(*args, **kwargs)

    def close(self):
        """will close ALL known CSMLoggers, including active and old"""
        if not self._activeCsmLogger:
            raise RuntimeError("(csmlog) setup() must be called first!")

        self._activeCsmLogger.close()
        self._activeCsmLogger = None

        for i in self._oldCsmLoggers:
            i.close()

        self._oldCsmLoggers.clear()

    def getCSMLogger(self):
        return self._activeCsmLogger

    def enableConsoleLogging(self, *args, **kwargs):
        if not self._activeCsmLogger:
            raise RuntimeError("(csmlog) setup() must be called first!")

        return self._activeCsmLogger.enableConsoleLogging(*args, **kwargs)

    def disableConsoleLogging(self, *args, **kwargs):
        if not self._activeCsmLogger:
            raise RuntimeError("(csmlog) setup() must be called first!")

        return self._activeCsmLogger.disableConsoleLogging(*args, **kwargs)

    def setFormatter(self, formatter=None):
        if not self._activeCsmLogger:
            raise RuntimeError("(csmlog) setup() must be called first!")

        self._activeCsmLogger.setFormatter(formatter)
        for logger in self._oldCsmLoggers:
            logger.setFormatter(formatter)

    def setup(
        self,
        appName,
        clearLogs=False,
        udpLogging=True,
        googleSheetShareEmail=None,
        modifyChildLoggersFunc=None,
    ):
        """must be called to setup the logger. Passes args to CSMLogger's constructor"""

        if self._activeCsmLogger is not None:
            self._activeCsmLogger.parentLogger.debug(
                "CSMLogger was already setup. Swapping to appName: %s." % appName
            )
            self._oldCsmLoggers.append(self._activeCsmLogger)

        self._activeCsmLogger = CSMLogger(
            appName,
            clearLogs,
            udpLogging,
            googleSheetShareEmail,
            modifyChildLoggersFunc,
        )
        self._activeCsmLogger.parentLogger.debug("==== %s is starting ====" % appName)


# this will also publish all public methods to globals() for this file.
_csmLoggerManager = _CSMLoggerManager()

# legacy alias for setup()
CSMLogger.setup = setup
