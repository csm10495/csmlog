'''
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2020) - Charles Machalow
'''

import contextlib
import datetime
import logging.handlers
import os
import pathlib
import pickle
import socket
import sys
import time
import threading

import gspread
import gspread.auth

SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/spreadsheets']

CREDENTIALS_DIR = pathlib.Path.home()

CREDENTIALS_FILE = CREDENTIALS_DIR / '.gcreds.json'
CREDENTIALS_CACHE = CREDENTIALS_DIR / 'authorized_user.json'

DEFAULT_LOG_WORKSHEET_NAME = 'csmlog'

GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH = 50_000

LOGGER_SPREADSHEET_PREFIX = f'csmlog/{socket.gethostname()}/'

MAX_EVENTS_TO_PROCESS_PER_INTERVAL = 200_000

ROWS_DELETE_WHEN_OUT_OF_SPACE = 200_000

_GSPREAD = None

@contextlib.contextmanager
def _monkeypatch(mod, name, value):
    real_val = getattr(mod, name)
    setattr(mod, name, value)
    try:
        yield
    finally:
        setattr(mod, name, real_val)

def _login_and_get_gspread(credentials_file):
    ''' login and get a Sheets instance. Will prompt for login if not done before '''
    global _GSPREAD
    if not _GSPREAD:
        if not os.path.isfile(credentials_file):
            raise FileNotFoundError(f"{credentials_file} should exist before using GSheetsHandler")

        try:
            _GSPREAD = gspread.service_account(credentials_file)
            _GSPREAD._login_type = 'service_account'
        except ValueError:
            # maybe we were given oauth client id instead

            # it would be cool if we could give a custom creds path, so improvise and make it allow this.
            with _monkeypatch(gspread.auth, 'DEFAULT_CREDENTIALS_FILENAME', CREDENTIALS_FILE):
                with _monkeypatch(gspread.auth, 'DEFAULT_AUTHORIZED_USER_FILENAME', CREDENTIALS_CACHE):
                    _GSPREAD = gspread.oauth()

            _GSPREAD._login_type = 'user_oauth'

    return _GSPREAD

class GSheetsHandler(logging.StreamHandler):
    ''' Special logging handler to send events to a Google Sheet '''

    def __init__(self, logger_name, share_email=None, min_time_per_process_loop=2, credentials_file=CREDENTIALS_FILE):
        self.logger_name = logger_name
        self.gspread = _login_and_get_gspread(credentials_file)

        self.workbook_name = LOGGER_SPREADSHEET_PREFIX + self.logger_name
        try:
            self.workbook = self.gspread.open(self.workbook_name)
        except gspread.SpreadsheetNotFound:
            self.workbook = self.gspread.create(self.workbook_name)

        # Ensure there is a log sheet
        self._ensure_default_sheet()

        # delete sheet1
        worksheet_names = [a.title for a in self.workbook.worksheets()]
        if 'Sheet1' in worksheet_names:
            self.workbook.del_worksheet(self.workbook.worksheet('Sheet1'))

        self.share_email = share_email
        if self.share_email:
            self._make_owner_if_not_already()

        self.min_time_per_process_loop = min_time_per_process_loop

        # rows that have not been added yet
        self._pending_rows = []
        self._pending_rows_mutex = threading.Lock()

        # start processing thread
        self._processing_thread = threading.Thread(target=self._periodically_process_pending_rows, daemon=True)
        self._processing_thread.start()

        logging.StreamHandler.__init__(self)

    def __repr__(self):
        return f'<GSheetsHandler {self.logger_name}>'

    def _make_owner_if_not_already(self):
        for p in self.workbook.list_permissions():
            if p['emailAddress'] == self.share_email and p['role'] == 'owner' and p['type'] == 'user':
                return

        self.workbook.share(self.share_email, perm_type='user', role='owner')

    def _ensure_default_sheet(self):
        try:
            self.workbook.worksheet(DEFAULT_LOG_WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            self.workbook.add_worksheet(DEFAULT_LOG_WORKSHEET_NAME, 1, 1)

        self.sheet = self.workbook.worksheet(DEFAULT_LOG_WORKSHEET_NAME)

    def _periodically_process_pending_rows(self):
        while True:
            before = time.time()
            self.process_pending_rows()
            after = time.time()

            if after - before > self.min_time_per_process_loop:
                sleep_time = 0
            else:
                sleep_time = max(after - before, self.min_time_per_process_loop)

            time.sleep(sleep_time)

    def process_pending_rows(self):
        print ("Process start")
        try:
            pending_rows_copy = []
            with self._pending_rows_mutex:
                if self._pending_rows:
                    pending_rows_copy = self._pending_rows[:MAX_EVENTS_TO_PROCESS_PER_INTERVAL]

            if pending_rows_copy:
                print("STARTING APPEND")
                # todo: if this takes longer than self.min_time_per_process_loop, rotate sheets!

                self.sheet.append_rows(pending_rows_copy)
                print("ENDING APPEND")
                # clear processed
                with self._pending_rows_mutex:
                    self._pending_rows[:len(pending_rows_copy)] = []

                    if len(self._pending_rows) > 0:
                        print(f"Not empty... Size: {len(self._pending_rows)}")

            print ("Process end")
        except Exception as ex:
            # this would mean we should wait to write for a bit more.
            if 'RESOURCE_EXHAUSTED' in str(ex).upper():
                time.sleep(10)
                return

            # this would mean we have run out of room in this sheet... try to create a new sheet/go to the next one.
            if 'ABOVE THE LIMIT' in str(ex).upper() and 'INVALID_ARGUMENT' in str(ex).upper():
                print ("ROTATING BEGIN!")
                # todo, delete oldest sheet instead.
                self.sheet.delete_rows(1, ROWS_DELETE_WHEN_OUT_OF_SPACE)
                print ("ROTATING END!")
                return

            print (f"Exception in process_pending_rows(): {ex}", file=sys.stderr)

            # not sure what to do :/

    def emit(self, record):
        rows = [(record.asctime, record.levelname, record.pathname, record.funcName, record.lineno, record.msg),]

        if len(record.msg) > GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH:
            rows = []
            # split row into multiple
            for i in range(0, len(record.msg), GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH):
                rows.append((record.asctime, record.levelname, record.pathname, record.funcName, record.lineno, record.msg[i:i+GOOGLE_SHEETS_MAX_CELL_CHAR_LENGTH]),)

        with self._pending_rows_mutex:
            for row in rows:
                self._pending_rows.append(row)


"""
    def _rotate_workbooks(self):
        ''' called if the current default worksheet is too large. '''
        all_worksheets = self.workbook.worksheets()
        all_worksheets_names = reversed(sorted([a.title for a in all_worksheets if a.title.startswith(DEFAULT_LOG_WORKSHEET_NAME)]))

        def get_worksheet_by_name(name):
            for i in all_worksheets:
                if i.title == name:
                    return i

        for i in all_worksheets_names:
            num_or_nothing = i.split(DEFAULT_LOG_WORKSHEET_NAME)[1]
            try:
                num = int(num_or_nothing)
            except ValueError:
                num = -1

            num = num + 1

            get_worksheet_by_name(i).update_title(f'{DEFAULT_LOG_WORKSHEET_NAME}{num}')

        self._ensure_default_sheet()
"""
