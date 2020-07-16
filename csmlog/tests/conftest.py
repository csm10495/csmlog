import os
import pytest
import sys

THIS_FOLDER = os.path.abspath(os.path.dirname(__file__))
PARENT_FOLDER = os.path.abspath(os.path.join(THIS_FOLDER, os.path.pardir, os.path.pardir))
sys.path.insert(0, PARENT_FOLDER)
from csmlog import CSMLogger, getCSMLogger, close, UdpHandlerReceiver, LoggedSystemCall, setup

APPNAME = 'csmlog_test'

@pytest.fixture(scope="function")
def csmlog():
    setup(APPNAME, clearLogs=True)
    try:
        yield getCSMLogger()
    finally:
        close()
