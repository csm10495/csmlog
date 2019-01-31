'''
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2019) - Charles Machalow
'''

import logging
import logging.handlers
import os

class CSMLogger(object):
    '''
    object to wrap logging logic
    '''
    theLogger = None # class-obj for the used logger

    def __init__(self, appName):
        self.appName = appName
        self.parentLogger = self.__getParentLogger()

    def getLogger(self, name):
        loggerName = '%s.%s' % (self.appName, name) # make this a sublogger of the whole app
        return self.__getLoggerWithName(loggerName)

    def __getParentLogger(self):
        return self.__getLoggerWithName(self.appName)

    def __getLoggerWithName(self, loggerName):
        logger = logging.getLogger(loggerName)
        logger.setLevel(1) # log all

        logFolder = self.getDefaultSaveDirectory()

        logFile = os.path.join(logFolder, loggerName + ".txt")

        formatter = logging.Formatter('%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s')

        rfh = logging.handlers.RotatingFileHandler(logFile, maxBytes=1024*1024*8, backupCount=10)
        rfh.setFormatter(formatter)
        logger.addHandler(rfh)

        return logger

    def getDefaultSaveDirectory(self):
        if os.name == 'nt':
            logFolder = os.path.join(os.path.expandvars("%APPDATA%"), self.appName)
        else:
            logFolder = os.path.join('/var/log/', self.appName)

        if not os.path.isdir(logFolder):
            os.makedirs(logFolder)

        return logFolder

    @classmethod
    def setup(cls, appName, *args, **kwargs):
        ''' must be called to setup the logger. Passes args to CSMLogger's constructor '''
        if cls.theLogger:
            raise RuntimeError("CSMLogger was already setup. It can only be setup once!")
        CSMLogger.theLogger = CSMLogger(appName, *args, **kwargs)
        CSMLogger.theLogger.parentLogger.debug("==== %s is starting ====" % appName)


# the following helper logic only works if the entire application is for one logging folder.
#  not quite sure if it would work with multiple CSMLoggers with different app names

def getLogger(*args, **kwargs):
    if not CSMLogger.theLogger:
        raise RuntimeError("CSMLogger.setup() must be called first!")

    return CSMLogger.theLogger.getLogger(*args, **kwargs)

setup = CSMLogger.setup