import io
import os
import subprocess
import sys
import threading
import time

import pathlib
import pytest

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from conftest import APPNAME, CSMLogger, UdpHandlerReceiver, LoggedSystemCall

def test_get_logger_and_clear_logs(csmlog):

    tmp = csmlog.getLogger('tmp')
    for i in range(100):
        tmp.debug('hey debug')
        tmp.info('hey info')

    loggerFile = os.path.join(csmlog.getDefaultSaveDirectory(), APPNAME + '.' + 'tmp.txt')
    assert os.path.isfile(loggerFile)
    with open(loggerFile, 'r') as f:
        txt = f.read()

    assert 'hey debug' in txt
    assert 'hey info' in txt

    # parent should have text also
    parentFile = os.path.join(csmlog.getDefaultSaveDirectory(), APPNAME + '.txt')
    assert os.path.isfile(parentFile)

    with open(parentFile, 'r') as f:
        txt = f.read()

    assert 'hey debug' in txt
    assert 'hey info' in txt

    csmlog.close()
    csmlog.clearLogs()

    assert not os.path.isfile(loggerFile)

def test_multi_setup_passes_and_moves(csmlog):
    ''' the final call to setup() is the one that prevails. '''

    first_save_directory_path = csmlog.getDefaultSaveDirectory()
    logger = csmlog.getLogger('test1')
    logger.debug("hello1")

    assert 'hello1' in pathlib.Path(logger.logFile).read_text()
    CSMLogger.setup('test2')

    # SAME LOGGER AS ABOVE... SHOULD STILL POINT TO ORIGINAL SPOT
    logger.debug('hello2')
    assert 'hello1' in pathlib.Path(logger.logFile).read_text()
    assert 'hello2' in pathlib.Path(logger.logFile).read_text()

    # this is a brand new logger. It should ONLY go to the new location
    logger2 = csmlog.getLogger('test2')
    logger2.debug('hello3')
    assert 'hello1' not in pathlib.Path(logger2.logFile).read_text()
    assert 'hello2' not in pathlib.Path(logger2.logFile).read_text()
    assert 'hello3' in pathlib.Path(logger2.logFile).read_text()

    logger.debug('hello4')
    assert 'hello1' in pathlib.Path(logger.logFile).read_text()
    assert 'hello2' in pathlib.Path(logger.logFile).read_text()
    assert 'hello3' not in pathlib.Path(logger.logFile).read_text()
    assert 'hello4' in pathlib.Path(logger.logFile).read_text()

    assert 'hello1' not in pathlib.Path(logger2.logFile).read_text()
    assert 'hello2' not in pathlib.Path(logger2.logFile).read_text()
    assert 'hello3' in pathlib.Path(logger2.logFile).read_text()
    assert 'hello4' not in pathlib.Path(logger2.logFile).read_text()

def test_sending_to_stderr(csmlog):
    sys.stderr = io.StringIO()
    logger = csmlog.getLogger("tmp")

    try:
        csmlog.enableConsoleLogging()
        logger.debug("test")
        csmlog.disableConsoleLogging()
        logger.debug("failure")

    finally:
        output = sys.stderr.getvalue()
        sys.stderr = sys.__stderr__

    assert "test" in output
    assert 'failure' not in output

def test_sending_to_alt_stream(csmlog):
    sys.stdout = io.StringIO()
    logger = csmlog.getLogger("tmp")

    try:
        csmlog.enableConsoleLogging(stream=sys.stdout)
        logger.debug("test")
        csmlog.disableConsoleLogging()
        logger.debug("failure")

    finally:
        output = sys.stdout.getvalue()
        sys.stdout = sys.__stdout__

    assert "test" in output
    assert 'failure' not in output

def test_2_enables_disables_first(csmlog):
    sys.stderr = io.StringIO()
    sys.stdout = io.StringIO()
    logger = csmlog.getLogger("tmp")

    try:
        csmlog.enableConsoleLogging(stream=sys.stderr)

        # only sys.stdout is active
        csmlog.enableConsoleLogging(stream=sys.stdout)
        logger.debug("test")
        csmlog.disableConsoleLogging()
        logger.debug("failure")

    finally:
        stderrOut = sys.stderr.getvalue()
        stdoutOut = sys.stdout.getvalue()
        sys.stderr = sys.__stderr__
        sys.stdout = sys.__stdout__

    assert "test" not in stderrOut
    assert 'failure' not in stderrOut

    assert "test" in stdoutOut
    assert 'failure' not in stdoutOut

def test_udp_logging(csmlog):
    # create reciever
    udpRecv = UdpHandlerReceiver()
    thread = threading.Thread(target=udpRecv.recieveForever)
    thread.start()

    logger = csmlog.getLogger("tmp")
    logger.debug("bleh" * 1000)

    for i in range(5):
        if udpRecv.getBuffer().count("bleh") == 1000:
            break

        # technically it may take a moment to appear in the buffer
        time.sleep(.1)
    else:
        assert udpRecv.getBuffer().count("bleh") == 1000

    udpRecv.requestStop()
    thread.join()

def test_file_attribute(csmlog):
    logger = csmlog.getLogger(__file__)
    logger.debug('hi')

def test_logged_system_call(csmlog):
    tmp = csmlog.getLogger('tmp')

    sysCall = LoggedSystemCall(tmp)
    assert sysCall.call("echo hi", shell=True) == 0
    assert 'hi' in sysCall.check_output('echo hi', shell=True)

    assert sysCall.call("easdsadcho hi", shell=True) != 0

    with pytest.raises(subprocess.CalledProcessError):
        sysCall.check_output('easdsadcho hi', shell=True)

    loggerFile = os.path.join(csmlog.getDefaultSaveDirectory(), APPNAME + '.' + 'tmp.txt')
    assert os.path.isfile(loggerFile)
    with open(loggerFile, 'r') as f:
        txt = f.read()

    assert 'hi' in txt
    assert 'easdsadcho' in txt

def test_added_attrs_on_logger(csmlog):
    ''' we add in the logFolder/logFile to the python logger. Make sure they are there '''
    tmp = csmlog.getLogger('tmp2')
    assert pathlib.Path(tmp.logFolder).is_dir()

    # file shouldn't exist till a log statement happens
    assert not pathlib.Path(tmp.logFile).is_file()
    tmp.info("hi")
    assert pathlib.Path(tmp.logFile).is_file()
