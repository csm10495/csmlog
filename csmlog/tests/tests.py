import io
import os
import sys

import pytest

from conftest import APPNAME, CSMLogger

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

def test_multi_setup_fails(csmlog):
    # can't setup again
    with pytest.raises(RuntimeError):
        CSMLogger.setup('round 2')

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
    logger = csmlog.getLogger("tmp")

    try:
        csmlog.enableConsoleLogging(stream=sys.stderr)

        # only sys.stdout is active
        csmlog.enableConsoleLogging(stream=sys.stdout)
        logger.debug("test")
        csmlog.disableConsoleLogging()
        logger.debug("failure")

    finally:
        output = sys.stderr.getvalue()
        sys.stderr = sys.__stdout__

    assert "test" not in output
    assert 'failure' not in output