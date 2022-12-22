"""
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2020) - Charles Machalow
"""

import contextlib
import datetime
import logging.handlers
import os
import pathlib
import pickle
import re
import socket
import sys
import time
import threading
import traceback
import unittest

import gspread
import gspread.auth

SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/spreadsheets",
]

CREDENTIALS_DIR = pathlib.Path.home()

CREDENTIALS_FILE = CREDENTIALS_DIR / ".gcreds.json"
CREDENTIALS_CACHE = CREDENTIALS_DIR / "authorized_user.json"

DEFAULT_LOG_WORKSHEET_NAME = "csmlog"

GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH = 50_000

LOGGER_SPREADSHEET_PREFIX = f"csmlog/{socket.gethostname()}/"

MAX_EVENTS_TO_PROCESS_PER_INTERVAL = 10_000

# soft limit to lead to a new sheet being used
MAX_EVENTS_TO_SPLIT_TO_NEW_SHEET = 100_000

MAX_OLD_LOG_SHEETS = 10

_GSPREAD = None

## Debug Code ==
_DEBUG = False


def _debug_print(s):
    if _DEBUG:
        print(s, file=sys.stderr)


def _debug_wrap(func):
    """a simple decorator to print function start/end time, used for debug"""

    def wrapper(*args, **kwargs):
        _debug_print(f"{datetime.datetime.now()} - About to run:  {func.__name__}")
        ret_val = func(*args, **kwargs)
        _debug_print(f"{datetime.datetime.now()} - Completed run: {func.__name__}")
        return ret_val

    return wrapper


## Debug Code ==

## Exceptions ==
class ResourceExhaustedError(RuntimeError):
    """raised if Google Sheets says we have talked to it too much"""

    pass


class WorkbookSpaceNeededError(RuntimeError):
    """raised if we need more space in the workbook"""

    pass


class WorksheetRotationNeededError(RuntimeError):
    """raised if we need to rotate to a new worksheet within this workbook"""

    pass


## Exceptions ==

## Misc Utils ==
@contextlib.contextmanager
def _monkeypatch(mod, name, value):
    """simple contextmanager for monkeypatching an object"""
    with unittest.mock.patch.object(mod, name, value):
        yield


def _natural_sort_worksheet(x):
    """helper to sort worksheets naturally"""
    l = re.findall(r"\d+$", x.title)
    if l:
        return int(l[0])

    return -1


## Misc Utils ==

## Resource Exhaustion Handling ==
@_debug_wrap
def _handle_resource_exhausted_error():
    """called whenever Google Sheets says we have exhausted our resources (API rate limiting)"""
    _debug_print(
        "Traceback that led to resource exhaustion handling: " + traceback.format_exc()
    )
    time.sleep(3)


def _wrap_for_resource_exhausted(func):
    """a decorator to auto-retry the function if it detects a resource exhaustion exception"""

    def wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as ex:
                if "RESOURCE_EXHAUSTED" in str(ex):
                    _debug_print(ex)
                    _handle_resource_exhausted_error()
                    continue
                raise

    return wrapper


class _WrapperForResourceExhaustionHandling:
    """
    A special object that takes in a gspread-based object. All calls are passed through to that object.
    When callables are given back, they will be wrapped to auto-retry on resource exhaustion
    """

    def __init__(self, gspread):
        self._gspread = gspread

    def __repr__(self):
        return repr(self._gspread)

    def __getattribute__(self, name):
        if name != "_gspread":
            thing = getattr(self._gspread, name)
            if callable(thing):
                return _wrap_for_resource_exhausted(thing)
            return thing
        else:
            return object.__getattribute__(self, name)


## Resource Exhaustion Handling ==


def _login_and_get_gspread(credentials_file):
    """login and get a Sheets instance. Will prompt for login if not done before"""
    global _GSPREAD
    if not _GSPREAD:
        if not os.path.isfile(credentials_file):
            raise FileNotFoundError(
                f"{credentials_file} should exist before using GSheetsHandler"
            )

        try:
            _GSPREAD = gspread.service_account(credentials_file)
            _GSPREAD._login_type = "service_account"
        except ValueError:
            # maybe we were given oauth client id instead

            # it would be cool if we could give a custom creds path, so improvise and make it allow this.
            with _monkeypatch(
                gspread.auth, "DEFAULT_CREDENTIALS_FILENAME", CREDENTIALS_FILE
            ):
                with _monkeypatch(
                    gspread.auth, "DEFAULT_AUTHORIZED_USER_FILENAME", CREDENTIALS_CACHE
                ):
                    _GSPREAD = gspread.oauth()

            _GSPREAD._login_type = "user_oauth"

    return _GSPREAD


class GSheetsHandler(logging.StreamHandler):
    """Special logging handler to send events to a Google Sheet"""

    def __init__(
        self,
        logger_name,
        share_email=None,
        min_time_per_process_loop=1,
        max_time_per_process_loop=5,
        credentials_file=CREDENTIALS_FILE,
        start_processing_thread=True,
    ):
        """initializer. start_processing_thread should be True unless you don't actually want to process log events
        share_email can be an email address to share the Google Sheet workbook with."""
        self.logger_name = logger_name
        self.gspread = _WrapperForResourceExhaustionHandling(
            _login_and_get_gspread(credentials_file)
        )

        self.workbook_name = LOGGER_SPREADSHEET_PREFIX + self.logger_name
        try:
            self.workbook = _WrapperForResourceExhaustionHandling(
                self.gspread.open(self.workbook_name)
            )
        except gspread.SpreadsheetNotFound:
            self.workbook = _WrapperForResourceExhaustionHandling(
                self.gspread.create(self.workbook_name)
            )

        # Ensure there is a log sheet
        self._ensure_default_sheet()

        # delete sheet1
        worksheet_names = [a.title for a in self.workbook.worksheets()]
        if "Sheet1" in worksheet_names:
            self.workbook.del_worksheet(self.workbook.worksheet("Sheet1"))

        self.share_email = share_email
        if self.share_email:
            self._make_owner_if_not_already()

        self.min_time_per_process_loop = min_time_per_process_loop
        self.max_time_per_process_loop = max_time_per_process_loop

        # rows that have not been added yet
        self._pending_rows = []
        self._pending_rows_mutex = threading.Lock()

        # keep track of the amount of time it took to add rows
        self._add_rows_time = 0

        # start processing thread
        self._start_processing_thread = start_processing_thread
        self._run_process_pending_rows = True
        self._processing_thread = threading.Thread(
            target=self._periodically_process_pending_rows, daemon=True
        )
        if self._start_processing_thread:
            self._processing_thread.start()

        logging.StreamHandler.__init__(self)

    def __repr__(self):
        return f"<GSheetsHandler {self.logger_name}>"

    def _make_owner_if_not_already(self):
        for p in self.workbook.list_permissions():
            if (
                p["emailAddress"] == self.share_email
                and p["role"] == "owner"
                and p["type"] == "user"
            ):
                return

        self.workbook.share(self.share_email, perm_type="user", role="owner")

    def _ensure_default_sheet(self):
        try:
            self.sheet = _WrapperForResourceExhaustionHandling(
                self.workbook.worksheet(DEFAULT_LOG_WORKSHEET_NAME)
            )
        except gspread.WorksheetNotFound:
            self.sheet = _WrapperForResourceExhaustionHandling(
                self.workbook.add_worksheet(DEFAULT_LOG_WORKSHEET_NAME, 1, 1)
            )

        self.rows_in_active_sheet = self.sheet.row_count

    @_debug_wrap
    def _rotate_to_new_sheet_in_workbook(self):
        all_worksheets = sorted(self.workbook.worksheets(), key=_natural_sort_worksheet)
        all_worksheets_names = reversed(
            [
                a.title
                for a in all_worksheets
                if a.title.startswith(DEFAULT_LOG_WORKSHEET_NAME)
            ]
        )

        def get_worksheet_by_name(name):
            for i in all_worksheets:
                if i.title == name:
                    return i

        new_sheet_list = []
        for i in all_worksheets_names:
            num_or_nothing = i.split(DEFAULT_LOG_WORKSHEET_NAME)[1]
            try:
                num = int(num_or_nothing)
            except ValueError:
                num = -1

            num = num + 1

            wks = _WrapperForResourceExhaustionHandling(get_worksheet_by_name(i))

            new_name = f"{DEFAULT_LOG_WORKSHEET_NAME}{num}"
            wks.update_title(new_name)
            new_sheet_list.append(wks)

        # remove excess old log sheets
        sheets_to_remove = list(reversed(new_sheet_list))[MAX_OLD_LOG_SHEETS:]
        for sheet in sheets_to_remove:
            _debug_print(f"Deleting sheet: {sheet}")
            self.workbook.del_worksheet(sheet)
            new_sheet_list.remove(sheet)

        self._ensure_default_sheet()

        # make the order newest to oldest
        wks_in_order = [self.sheet] + list(reversed(new_sheet_list))
        self.workbook.reorder_worksheets(wks_in_order)
        _debug_print(f"Sheet order: {wks_in_order}")

        # reset the add row time to not let this continually get run.
        #  the add_rows_time will get set by next add to the sheet
        self._add_rows_time = 0

    @_debug_wrap
    def _handle_workbook_space_needed_error(self):
        worksheets = sorted(self.workbook.worksheets(), key=_natural_sort_worksheet)
        oldest = worksheets[-1]
        _debug_print(f"Removing sheet: {oldest}")
        self.workbook.del_worksheet(oldest)

    def _add_rows_to_active_sheet(self, rows):
        """adds the given rows to the currently active sheet."""
        start = time.time()
        try:
            ret = self.sheet.append_rows(rows)
            self.rows_in_active_sheet += len(rows)
            return ret
        except Exception as ex:
            # this would mean we should wait to write for a bit more.
            if "RESOURCE_EXHAUSTED" in str(ex).upper():
                raise ResourceExhaustedError(str(ex))

            # api is down? Act the same as resource exhausted
            if "UNAVAILABLE" in str(ex).upper():
                raise ResourceExhaustedError(str(ex))

            # this would mean we have run out of room in this sheet... try to create a new sheet/go to the next one.
            if (
                "ABOVE THE LIMIT" in str(ex).upper()
                and "INVALID_ARGUMENT" in str(ex).upper()
            ):
                raise WorkbookSpaceNeededError(str(ex))

            raise
        finally:
            end = time.time()
            self._add_rows_time = end - start

    def _calculate_periodic_loop_sleep_time(self, time_for_process):
        """calculates the amount of time we should sleep based of the time it took for the
        process loop to complete"""
        if time_for_process > self.min_time_per_process_loop:
            sleep_time = 0
        else:
            sleep_time = self.min_time_per_process_loop - time_for_process

        return sleep_time

    def _periodically_process_pending_rows(self):
        """ran in a thread to periodically take rows and write them to sheets.
        Also may perform other actions such as rotation to keep things working smooth"""
        while self._run_process_pending_rows:
            try:
                before = time.time()
                try:
                    self.process_pending_rows()
                except ResourceExhaustedError:
                    _handle_resource_exhausted_error()
                    continue
                except WorkbookSpaceNeededError:
                    self._handle_workbook_space_needed_error()
                    continue
                except Exception as ex:
                    _debug_print(f"Exception in process_pending_rows(): {ex}")
                    continue

                if (
                    self._add_rows_time > self.max_time_per_process_loop
                    or self.rows_in_active_sheet > MAX_EVENTS_TO_SPLIT_TO_NEW_SHEET
                ):
                    # its taking too long to add rows to the sheet. Rotate
                    _debug_print(
                        f"triggering rotation as the add_rows_time was: {self._add_rows_time} and rows_in_active_sheet was {self.rows_in_active_sheet}"
                    )
                    self._rotate_to_new_sheet_in_workbook()

                after = time.time()
                time.sleep(self._calculate_periodic_loop_sleep_time(after - before))
            except Exception as ex:
                _debug_print(
                    f"Exception made it to the top of the loop in _periodically_process_pending_rows(): {traceback.format_exc()}"
                )
                _handle_resource_exhausted_error()
                continue

    def process_pending_rows(self):
        pending_rows_copy = []
        with self._pending_rows_mutex:
            if self._pending_rows:
                # make a copy to not hold up other mutex users
                pending_rows_copy = self._pending_rows[
                    :MAX_EVENTS_TO_PROCESS_PER_INTERVAL
                ]

        if pending_rows_copy:
            # an exception on this means things were NOT added to the sheet
            self._add_rows_to_active_sheet(pending_rows_copy)

            # clear processed
            with self._pending_rows_mutex:
                self._pending_rows[: len(pending_rows_copy)] = []

                # if we start printing here, we are logging faster than uploading (falling behind)
                if len(self._pending_rows) > 0:
                    _debug_print(
                        f"self._pending_rows was not empty... Size: {len(self._pending_rows)}"
                    )

    def emit(self, record):
        """
        Called when a log record is to be sent. This just queues the events to be written to Sheets.
            Will also break up msgs that are longer than the max cell size allowed by Google.
        """
        full_record_msg_str = str(record.msg)
        rows = [
            (
                record.asctime,
                record.levelname,
                record.pathname,
                record.funcName,
                record.lineno,
                full_record_msg_str,
            ),
        ]

        if len(full_record_msg_str) > GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH:
            rows = []
            # split row into multiple
            for i in range(
                0, len(full_record_msg_str), GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH
            ):
                rows.append(
                    (
                        record.asctime,
                        record.levelname,
                        record.pathname,
                        record.funcName,
                        record.lineno,
                        full_record_msg_str[i : i + GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH],
                    ),
                )

        with self._pending_rows_mutex:
            for row in rows:
                self._pending_rows.append(row)

    def flush(self):
        """Call to wait until all events have been reflected to sheets.
        Note: This can hang forever if we can't write to Google Sheets as fast as logging is currently happening"""
        while self._processing_thread.is_alive():
            with self._pending_rows_mutex:
                if len(self._pending_rows) == 0:
                    break
            time.sleep(0.1)

    def close(self):
        """requests that we stop processing new pending rows"""
        self._run_process_pending_rows = False
