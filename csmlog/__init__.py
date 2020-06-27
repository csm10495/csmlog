'''
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2020) - Charles Machalow
'''

import logging
import logging.handlers
import os
import shutil
import sys
import uuid
from pathlib import Path

from csmlog.system_call import LoggedSystemCall
from csmlog.udp_handler import UdpHandler
from csmlog.udp_handler_receiver import UdpHandlerReceiver

__version__ = '0.10'


class CSMLogger(object):
    '''
    object to wrap logging logic
    '''
    theLogger = None # class-obj for the used logger

    def __init__(self, appName, clearLogs=False, udpLogging=True):
        self.appName = appName
        self.udpLogging = udpLogging

        if clearLogs:
            self.clearLogs()

        self.parentLogger = self.__getParentLogger()
        self.consoleLoggingStream = None
        self._loggers = [self.parentLogger] # keep track of all loggers

    def close(self):
        for logger in self._loggers:
            for handler in logger.handlers[:]:
                handler.close()
                logger.removeHandler(handler)

        self._loggers = []

    def getLogger(self, name):
        name = os.path.basename(name)
        loggerName = '%s.%s' % (self.appName, name) # make this a sublogger of the whole app
        logger = self.__getLoggerWithName(loggerName)
        self._loggers.append(logger)
        logger.sysCall = LoggedSystemCall(logger)
        return logger

    def __getParentLogger(self):
        logger = self.__getLoggerWithName(self.appName)
        if self.udpLogging:
            handler = UdpHandler()
            handler.setFormatter(self.getFormatter())
            logger.addHandler(handler)

        return logger

    def __getLoggerWithName(self, loggerName):
        logger = logging.getLogger(loggerName)
        logger.setLevel(1) # log all

        logFolder = self.getDefaultSaveDirectory()

        logFile = os.path.join(logFolder, loggerName + ".txt")

        formatter = self.getFormatter()

        rfh = logging.handlers.RotatingFileHandler(logFile, maxBytes=1024*1024*8, backupCount=10)
        rfh.setFormatter(formatter)
        logger.addHandler(rfh)

        return logger

    def getFormatter(self):
        return logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')

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

    @classmethod
    def getDefaultSaveDirectoryWithName(cls, appName):
        if os.name == 'nt':
            logFolder = os.path.join(os.path.expandvars("%APPDATA%"), appName)
        else:
            tmpPath = Path(f'/var/log/{uuid.uuid4()}')
            try:
                tmpPath.touch()
                tmpPath.unlink()
                tmpPath = tmpPath.parent
            except PermissionError:
                # can't use /var/log... try using ~/log/
                tmpPath = Path.home() / 'log'
                tmpPath.mkdir(exist_ok=True)

            logFolder = tmpPath / appName

        if not os.path.isdir(logFolder):
            os.makedirs(logFolder)

        return logFolder

    def clearLogs(self):
        shutil.rmtree(self.getDefaultSaveDirectory())

        # recreate empty folder
        self.getDefaultSaveDirectory()

    @classmethod
    def setup(cls, appName, clearLogs=False, udpLogging=True):
        ''' must be called to setup the logger. Passes args to CSMLogger's constructor '''
        if getattr(cls, 'theLogger', None):
            CSMLogger.theLogger.parentLogger.debug("CSMLogger was already setup. It can only be setup once! ... not setting up appName: %s" % appName)
            return

        CSMLogger.theLogger = CSMLogger(appName, clearLogs, udpLogging)
        CSMLogger.theLogger.parentLogger.debug("==== %s is starting ====" % appName)


# the following helper logic only works if the entire application is for one logging folder.
#  not quite sure if it would work with multiple CSMLoggers with different app names

def getLogger(*args, **kwargs):
    if not CSMLogger.theLogger:
        raise RuntimeError("CSMLogger.setup() must be called first!")

    return CSMLogger.theLogger.getLogger(*args, **kwargs)

def close():
    if not CSMLogger.theLogger:
        raise RuntimeError("CSMLogger.setup() must be called first!")
    retVal = CSMLogger.theLogger.close()
    CSMLogger.theLogger = None
    return retVal

def getCSMLogger():
    return CSMLogger.theLogger

def enableConsoleLogging(*args, **kwargs):
    if not CSMLogger.theLogger:
        raise RuntimeError("CSMLogger.setup() must be called first!")

    return CSMLogger.theLogger.enableConsoleLogging(*args, **kwargs)

def disableConsoleLogging(*args, **kwargs):
    if not CSMLogger.theLogger:
        raise RuntimeError("CSMLogger.setup() must be called first!")

    return CSMLogger.theLogger.disableConsoleLogging(*args, **kwargs)

setup = CSMLogger.setup
