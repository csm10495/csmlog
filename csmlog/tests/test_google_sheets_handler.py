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
     _WrapperForResourceExhaustionHandling, _login_and_get_gspread, GSheetsHandler, ResourceExhaustedError, \
     WorkbookSpaceNeededError, MAX_EVENTS_TO_SPLIT_TO_NEW_SHEET, DEFAULT_LOG_WORKSHEET_NAME, LOGGER_SPREADSHEET_PREFIX

@pytest.fixture(scope="function")
def gsheets_handler():
    # remove logic for exhaustion handling
    with _monkeypatch(google_sheets_handler, '_WrapperForResourceExhaustionHandling', lambda x: x):
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
    with _monkeypatch(google_sheets_handler, '_WrapperForResourceExhaustionHandling', lambda x: x):
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
        w = _WrapperForResourceExhaustionHandling(Test())
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

    # send to pending rows (non-str)
    gsheets_handler.emit(get_record(123))
    assert gsheets_handler._pending_rows[-1][-1] == '123'

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
        with unittest.mock.patch.object(gsheets_handler_no_thread, '_handle_workbook_space_needed_error') as mock:
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
    assert gsheets_handler_no_thread._add_rows_time < 99 and gsheets_handler_no_thread._add_rows_time >= 0

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

def test_gsheets_handle_workbook_space_needed(gsheets_handler_no_thread):
    class FakeWorksheet:
        def __init__(self, title):
            self.title = title
        def __eq__(self, other):
            return self.title == other.title

    worksheets = [FakeWorksheet('log2'), FakeWorksheet('log0'), FakeWorksheet('log1')]
    with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'worksheets', return_value=worksheets) as mock:
        with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'del_worksheet') as del_wks_mock:
            gsheets_handler_no_thread._handle_workbook_space_needed_error()

            mock.assert_called_once()
            del_wks_mock.assert_called_once_with(FakeWorksheet('log2'))

def test_gsheets_init_creates_workbook_if_it_doesnt_exist(gsheets_handler_no_thread):
    with unittest.mock.patch.object(gsheets_handler_no_thread.gspread, 'open') as open_mock:
        with unittest.mock.patch.object(gsheets_handler_no_thread.gspread, 'create') as create_mock:
            gsheets_handler_no_thread.__init__('test')
            open_mock.assert_called_once_with(LOGGER_SPREADSHEET_PREFIX + 'test')
            create_mock.assert_not_called()

    with unittest.mock.patch.object(gsheets_handler_no_thread.gspread, 'open', side_effect=gspread.SpreadsheetNotFound()) as open_mock:
        with unittest.mock.patch.object(gsheets_handler_no_thread.gspread, 'create') as create_mock:
            gsheets_handler_no_thread.__init__('test')
            open_mock.assert_called_once_with(LOGGER_SPREADSHEET_PREFIX + 'test')
            create_mock.assert_called_once_with(LOGGER_SPREADSHEET_PREFIX + 'test')

def test_gsheets_init_calls_make_owner_if_not_already_if_email_given(gsheets_handler_no_thread):
    with unittest.mock.patch.object(gsheets_handler_no_thread, '_make_owner_if_not_already') as mock:
        gsheets_handler_no_thread.__init__('test')
        mock.assert_not_called()
        gsheets_handler_no_thread.__init__('test', share_email='testemail@bleh.net')
        mock.assert_called_once()

def test_gsheets_make_owner_if_not_already(gsheets_handler_no_thread):
    gsheets_handler_no_thread.share_email = 'bleh2@bleh.net'

    with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'list_permissions', return_value=[
        {
            'emailAddress' : 'bleh@bleh.net',
            'role' : 'owner',
            'type' : 'user',
        },
    ]) as list_permissions_mock:
        with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'share') as share_mock:
            gsheets_handler_no_thread._make_owner_if_not_already()
            list_permissions_mock.assert_called_once()
            share_mock.assert_called_once_with('bleh2@bleh.net', perm_type='user', role='owner')

            list_permissions_mock.reset_mock()
            share_mock.reset_mock()

            gsheets_handler_no_thread.share_email = 'bleh@bleh.net'
            gsheets_handler_no_thread._make_owner_if_not_already()
            list_permissions_mock.assert_called_once()
            share_mock.assert_not_called()

def test_gsheets_ensure_default_sheet(gsheets_handler_no_thread):
    class FakeWorksheet:
        def __init__(self, r):
            self.row_count = r

    with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'worksheet', return_value=FakeWorksheet(123)) as worksheet_mock:
        with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'add_worksheet') as add_worksheet_mock:
            gsheets_handler_no_thread._ensure_default_sheet()
            worksheet_mock.assert_called_once_with(DEFAULT_LOG_WORKSHEET_NAME)
            add_worksheet_mock.assert_not_called()
            assert gsheets_handler_no_thread.rows_in_active_sheet == 123

    with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'worksheet', side_effect=gspread.WorksheetNotFound()) as worksheet_mock:
        with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'add_worksheet', return_value=FakeWorksheet(432)) as add_worksheet_mock:
            gsheets_handler_no_thread._ensure_default_sheet()
            worksheet_mock.assert_called_once_with(DEFAULT_LOG_WORKSHEET_NAME)
            add_worksheet_mock.assert_called_once_with(DEFAULT_LOG_WORKSHEET_NAME, 1, 1)
            assert gsheets_handler_no_thread.rows_in_active_sheet == 432

class TestGSheetsSheetRotation:
    @classmethod
    def getFakeWorksheet(cls, suffix=''):
        class FakeWorksheet:
            def __init__(self, suffix):
                self.title = DEFAULT_LOG_WORKSHEET_NAME + str(suffix)
            def __repr__(self):
                return f'<FakeWorksheet: {self.title}>'
            def update_title(self, title):
                self.title = title
            def __eq__(self, other):
                return self.title == other.title
            def __hash__(self):
                return hash(self.title)

        return FakeWorksheet(suffix)

    def test_rotation_updates_titles(self, gsheets_handler_no_thread):
        def ensure_default_sheet():
            gsheets_handler_no_thread.sheet = self.getFakeWorksheet()

        # set to non-zero before testing
        gsheets_handler_no_thread._add_rows_time = 913

        worksheets = [self.getFakeWorksheet(5), self.getFakeWorksheet()]
        with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'worksheets', return_value=worksheets) as worksheets_mock:
            with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'reorder_worksheets') as reorder_worksheets_mock:
                with unittest.mock.patch.object(gsheets_handler_no_thread, '_ensure_default_sheet', side_effect=ensure_default_sheet) as ensure_default_sheet_mock:
                    gsheets_handler_no_thread._rotate_to_new_sheet_in_workbook()
                    worksheets_mock.assert_called_once()
                    reorder_worksheets_mock.assert_called_once_with([self.getFakeWorksheet(), self.getFakeWorksheet(0), self.getFakeWorksheet(6)])
                    ensure_default_sheet_mock.assert_called_once()

                    # check this was reset while here.
                    assert gsheets_handler_no_thread._add_rows_time == 0

    def test_rotation_removes_excess_old_sheets(self, gsheets_handler_no_thread):
        def ensure_default_sheet():
            gsheets_handler_no_thread.sheet = self.getFakeWorksheet()

        deleted_worksheets = []
        def del_worksheet(wks):
            deleted_worksheets.append(wks)

        worksheets = [self.getFakeWorksheet(5), self.getFakeWorksheet(), self.getFakeWorksheet(1), self.getFakeWorksheet(3), self.getFakeWorksheet(2)]
        with _monkeypatch(google_sheets_handler, 'MAX_OLD_LOG_SHEETS', 3):
            with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'worksheets', return_value=worksheets) as worksheets_mock:
                with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'reorder_worksheets') as reorder_worksheets_mock:
                    with unittest.mock.patch.object(gsheets_handler_no_thread, '_ensure_default_sheet', side_effect=ensure_default_sheet) as ensure_default_sheet_mock:
                        with unittest.mock.patch.object(gsheets_handler_no_thread.workbook, 'del_worksheet', side_effect=del_worksheet) as ensure_default_sheet_mock:
                            gsheets_handler_no_thread._rotate_to_new_sheet_in_workbook()

                            assert set(deleted_worksheets) == set([self.getFakeWorksheet(6), self.getFakeWorksheet(4)])
                            reorder_worksheets_mock.assert_called_once_with([gsheets_handler_no_thread.sheet, self.getFakeWorksheet(0), self.getFakeWorksheet(2), self.getFakeWorksheet(3)])
