import gspread
import os
import pathlib
import pytest
import sys
import unittest.mock

# fudge path to get the other files
THIS_FOLDER = os.path.abspath(os.path.dirname(__file__))
PARENT_FOLDER = os.path.abspath(os.path.join(THIS_FOLDER, os.path.pardir))
sys.path.insert(0, PARENT_FOLDER)

import google_sheets_handler
from google_sheets_handler import _monkeypatch, _natural_sort_worksheet, _wrap_for_resource_exhausted, \
     _WrapperForResourceExahustionHandling, _login_and_get_gspread, GSheetsHandler, ResourceExhaustedError, \
     WorkbookSpaceNeededError, MAX_EVENTS_TO_SPLIT_TO_NEW_SHEET

@pytest.fixture(scope="function")
def gsheets_handler():
    # remove logic for exhaustion handling
    with _monkeypatch(google_sheets_handler, '_WrapperForResourceExahustionHandling', lambda x: x):
        google_sheets_handler._GSPREAD = unittest.mock.MagicMock()
        try:
            g = GSheetsHandler('unit_tests', min_time_per_process_loop=0, start_processing_thread=False)
            g.rows_in_active_sheet = 0
            g._processing_thread.start()
            yield g
        finally:
            g.close()

@pytest.fixture(scope="function")
def gsheets_handler_no_thread():
    # remove logic for exhaustion handling
    with _monkeypatch(google_sheets_handler, '_WrapperForResourceExahustionHandling', lambda x: x):
        google_sheets_handler._GSPREAD = unittest.mock.MagicMock()
        try:
            g = GSheetsHandler('unit_tests', min_time_per_process_loop=0, start_processing_thread=False)
            g.rows_in_active_sheet = 0
            yield g
        finally:
            g.close()

def test_monkeypatch():
    class Test:
        pass

    t = Test()
    t.abc = '123'
    with _monkeypatch(t, 'abc', '456'):
        assert t.abc == '456'

    assert t.abc == '123'

def test_natural_sort_worksheet():
    class FakeWorksheet:
        def __init__(self, title):
            self.title = title

    assert _natural_sort_worksheet(FakeWorksheet('mine3')) == 3
    assert _natural_sort_worksheet(FakeWorksheet('mine')) == -1
    assert _natural_sort_worksheet(FakeWorksheet('mine0')) == 0

def test_wrap_for_resource_exhausted():
    global count
    count = 0
    def test():
        global count
        count += 1
        if count < 2:
            raise Exception("I see RESOURCE_EXHAUSTED")

    with unittest.mock.patch.object(google_sheets_handler, '_handle_resource_exhausted_error'):
        _wrap_for_resource_exhausted(test)()
    assert count == 2

    def test():
        raise ValueError("Other exception")
    with pytest.raises(ValueError):
        _wrap_for_resource_exhausted(test)()

def test_exhaustion_handling_object():
    class Test:
        def __init__(self):
            self.attr = 123
        def method(self):
            return True

    with unittest.mock.patch.object(google_sheets_handler, '_wrap_for_resource_exhausted') as mock:
        # callables should get wrapped
        w = _WrapperForResourceExahustionHandling(Test())
        assert w.attr == 123
        assert isinstance(w.attr, int)
        assert isinstance(w.method, unittest.mock.Mock)

def test_login_and_get_gspread():
    google_sheets_handler._GSPREAD = 45
    assert _login_and_get_gspread(None) == 45


    google_sheets_handler._GSPREAD = None
    with pytest.raises(FileNotFoundError):
        _login_and_get_gspread('/asdasd/asdffasfd/fgasfdfa/fdasfadsf/dsafadsfasdf/afds/cvx')

    google_sheets_handler._GSPREAD = None
    with unittest.mock.patch.object(gspread, 'service_account') as mock:
        ret = _login_and_get_gspread(__file__)
        assert isinstance(ret, unittest.mock.Mock)
        assert ret._login_type == 'service_account'

    google_sheets_handler._GSPREAD = None
    with unittest.mock.patch.object(gspread, 'service_account', side_effect=ValueError()):
        with unittest.mock.patch.object(gspread, 'oauth') as mock:
            ret = _login_and_get_gspread(__file__)
            mock.assert_called_once()
        assert isinstance(ret, unittest.mock.Mock)
        assert ret._login_type == 'user_oauth'
    google_sheets_handler._GSPREAD = None

def test_gsheets_handler_emit(gsheets_handler):
    def get_record(msg):
        record = unittest.mock.Mock()
        record.msg = msg
        return record

    # stop the thread
    gsheets_handler.close()
    gsheets_handler._processing_thread.join()

    # send to pending rows (normal)
    gsheets_handler.emit(get_record('hello'))
    assert gsheets_handler._pending_rows[-1][-1] == 'hello'

    # send to pending rows (too long, so should split)
    gsheets_handler.emit(get_record(('A' * google_sheets_handler.GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH) + 'B'))
    B = gsheets_handler._pending_rows[-1]
    A = gsheets_handler._pending_rows[-2]

    # all should match except msg
    assert A[:-1] == B[:-1]
    assert A[-1] == 'A' * google_sheets_handler.GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH
    assert B[-1] == 'B'

def test_gsheets_handler_close_stops(gsheets_handler):
    gsheets_handler._processing_thread.join(.1)
    assert gsheets_handler._processing_thread.is_alive()

    # stop the thread
    gsheets_handler.close()
    gsheets_handler._processing_thread.join(1)
    assert not gsheets_handler._processing_thread.is_alive()

def test_gsheets_handler_flush(gsheets_handler):
    def get_record(msg):
        record = unittest.mock.Mock()
        record.msg = msg
        return record

    for i in range(100):
        gsheets_handler.emit(get_record("bleh"))

    gsheets_handler.flush()
    with gsheets_handler._pending_rows_mutex:
        assert len(gsheets_handler._pending_rows) == 0

def test_gsheets_process_pending_rows(gsheets_handler_no_thread):
    # no rows means don't call _add_rows_to_active_sheet
    gsheets_handler_no_thread._pending_rows = []
    with unittest.mock.patch.object(gsheets_handler_no_thread, '_add_rows_to_active_sheet') as mock:
        gsheets_handler_no_thread.process_pending_rows()
    mock.assert_not_called()

    # more rows than max per interval, means call with max allowed and remove those from pending
    gsheets_handler_no_thread._pending_rows = [a for a in range(google_sheets_handler.MAX_EVENTS_TO_PROCESS_PER_INTERVAL * 2)]
    with unittest.mock.patch.object(gsheets_handler_no_thread, '_add_rows_to_active_sheet') as mock:
        gsheets_handler_no_thread.process_pending_rows()

    mock.assert_called_once_with([a for a in range(google_sheets_handler.MAX_EVENTS_TO_PROCESS_PER_INTERVAL)])
    assert len(gsheets_handler_no_thread._pending_rows) == google_sheets_handler.MAX_EVENTS_TO_PROCESS_PER_INTERVAL
    assert gsheets_handler_no_thread._pending_rows == [a for a in range(google_sheets_handler.MAX_EVENTS_TO_PROCESS_PER_INTERVAL, google_sheets_handler.MAX_EVENTS_TO_PROCESS_PER_INTERVAL*2)]

def test_gsheets_periodically_process_pending_rows_resource_exhausted(gsheets_handler_no_thread):
    def side_effect():
        gsheets_handler_no_thread.close()
        raise ResourceExhaustedError

    with unittest.mock.patch.object(gsheets_handler_no_thread, 'process_pending_rows', side_effect=side_effect):
        with unittest.mock.patch.object(google_sheets_handler, '_handle_resource_exhausted_error') as mock:
            gsheets_handler_no_thread._periodically_process_pending_rows()
            mock.assert_called_once()

def test_gsheets_periodically_process_pending_rows_workbook_space_needed(gsheets_handler_no_thread):
    def side_effect():
        gsheets_handler_no_thread.close()
        raise WorkbookSpaceNeededError

    with unittest.mock.patch.object(gsheets_handler_no_thread, 'process_pending_rows', side_effect=side_effect):
        with unittest.mock.patch.object(gsheets_handler_no_thread, '_handle_workspace_space_needed_error') as mock:
            gsheets_handler_no_thread._periodically_process_pending_rows()
            mock.assert_called_once()

def test_gsheets_periodically_process_pending_rows_other_error(gsheets_handler_no_thread):
    ''' other errors should hit _debug_print, but not raise to user '''
    def side_effect():
        gsheets_handler_no_thread.close()
        raise ValueError("bleh")

    with unittest.mock.patch.object(gsheets_handler_no_thread, 'process_pending_rows', side_effect=side_effect):
        gsheets_handler_no_thread._periodically_process_pending_rows()

def test_gsheets_periodically_process_pending_does_not_always_rotate(gsheets_handler_no_thread):
    def side_effect():
        gsheets_handler_no_thread.close()

    with unittest.mock.patch.object(gsheets_handler_no_thread, 'process_pending_rows', side_effect=side_effect) as k:
        with unittest.mock.patch.object(gsheets_handler_no_thread, '_rotate_to_new_sheet_in_workbook') as mock:
            gsheets_handler_no_thread._periodically_process_pending_rows()

    k.assert_called_once()
    mock.assert_not_called()

def test_gsheets_periodically_process_pending_rotate_on_taking_too_long(gsheets_handler_no_thread):
    def side_effect():
        gsheets_handler_no_thread.close()
        gsheets_handler_no_thread._add_rows_time = 99999

    with unittest.mock.patch.object(gsheets_handler_no_thread, 'process_pending_rows', side_effect=side_effect):
        with unittest.mock.patch.object(gsheets_handler_no_thread, '_rotate_to_new_sheet_in_workbook') as mock:
            gsheets_handler_no_thread._periodically_process_pending_rows()

    mock.assert_called_once()

def test_gsheets_periodically_process_pending_rotate_on_too_many_rows(gsheets_handler_no_thread):
    def side_effect():
        gsheets_handler_no_thread.close()
        gsheets_handler_no_thread.rows_in_active_sheet = 9999999999999

    with unittest.mock.patch.object(gsheets_handler_no_thread, 'process_pending_rows', side_effect=side_effect):
        with unittest.mock.patch.object(gsheets_handler_no_thread, '_rotate_to_new_sheet_in_workbook') as mock:
            gsheets_handler_no_thread._periodically_process_pending_rows()

    mock.assert_called_once()

def test_gsheets_periodically_process_pending_top_level_error_leads_to_resource_exhaustion_handling(gsheets_handler_no_thread):
    def side_effect():
        gsheets_handler_no_thread.close()

        # force rotation logic to hit
        gsheets_handler_no_thread.rows_in_active_sheet = 9999999999999

    with unittest.mock.patch.object(gsheets_handler_no_thread, 'process_pending_rows', side_effect=side_effect):
        with unittest.mock.patch.object(google_sheets_handler, '_handle_resource_exhausted_error') as mock:
            with unittest.mock.patch.object(gsheets_handler_no_thread, '_rotate_to_new_sheet_in_workbook', side_effect=EnvironmentError) as mock2:
                gsheets_handler_no_thread._periodically_process_pending_rows()

    mock.assert_called_once()
    mock2.assert_called_once()

def test_gsheets_calculate_periodic_loop_sleep_time(gsheets_handler_no_thread):
    gsheets_handler_no_thread.min_time_per_process_loop = 5
    assert gsheets_handler_no_thread._calculate_periodic_loop_sleep_time(10) == 0
    assert gsheets_handler_no_thread._calculate_periodic_loop_sleep_time(4) == 1

def test_gsheets_add_rows_to_active_sheet_sets_add_rows_time(gsheets_handler_no_thread):
    gsheets_handler_no_thread._add_rows_time = 99
    assert isinstance(gsheets_handler_no_thread._add_rows_to_active_sheet([]), unittest.mock.Mock)
    assert gsheets_handler_no_thread._add_rows_time < 99 and gsheets_handler_no_thread._add_rows_time > 0

def test_gsheets_add_rows_to_active_sheet_set_coerce_to_correct_exceptions(gsheets_handler_no_thread):
    with unittest.mock.patch.object(gsheets_handler_no_thread.sheet, 'append_rows', side_effect=Exception("RESOURCE_EXHAUSTED uh-oh")) as mock:
        with pytest.raises(ResourceExhaustedError):
            gsheets_handler_no_thread._add_rows_to_active_sheet([])
        mock.assert_called_once()

    with unittest.mock.patch.object(gsheets_handler_no_thread.sheet, 'append_rows', side_effect=Exception("UNAVAILABLE uh-oh")) as mock:
        with pytest.raises(ResourceExhaustedError):
            gsheets_handler_no_thread._add_rows_to_active_sheet([])
        mock.assert_called_once()

    with unittest.mock.patch.object(gsheets_handler_no_thread.sheet, 'append_rows', side_effect=Exception("INVALID_ARGUMENT YOU ARE ABOVE THE LIMIT")) as mock:
        with pytest.raises(WorkbookSpaceNeededError):
            gsheets_handler_no_thread._add_rows_to_active_sheet([])
        mock.assert_called_once()

    e = EnvironmentError("other thing")
    with unittest.mock.patch.object(gsheets_handler_no_thread.sheet, 'append_rows', side_effect=e) as mock:
        with pytest.raises(EnvironmentError):
            gsheets_handler_no_thread._add_rows_to_active_sheet([])
        mock.assert_called_once()
