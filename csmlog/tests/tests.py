import pytest
import os
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
