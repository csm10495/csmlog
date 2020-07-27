import datetime
import logging.handlers
import os
import pathlib
import pickle
import time
import threading

import gspread

SCOPES = ['https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/spreadsheets']

CREDENTIALS_FILE = pathlib.Path.home() / '.gcreds.json'
LOGGER_SPREADSHEET_PREFIX = 'csmlog/'

_GSPREAD = None

def _login_and_get_gspread(credentials_file):
    ''' login and get a Sheets instance. Will prompt for login if not done before '''
    global _GSPREAD
    if not _GSPREAD:
        if not os.path.isfile(credentials_file):
            raise FileNotFoundError(f"{credentials_file} should exist before using GSheetsHandler")
        _GSPREAD = gspread.service_account(credentials_file)
    return _GSPREAD

class GSheetsHandler(logging.StreamHandler):
    def __init__(self, logger_name, share_email=None, sleep_per_process_loop=2, credentials_file=CREDENTIALS_FILE):
        self.logger_name = logger_name
        self.gspread = _login_and_get_gspread(credentials_file)

        self.workbook_name = LOGGER_SPREADSHEET_PREFIX + self.logger_name
        try:
            self.workbook = self.gspread.open(self.workbook_name)
        except gspread.SpreadsheetNotFound:
            self.workbook = self.gspread.create(self.workbook_name)
        self.sheet = self.workbook.sheet1

        self.share_email = share_email
        if self.share_email:
            self._make_owner_if_not_already()

        self.sleep_per_process_loop = sleep_per_process_loop

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

    def _periodically_process_pending_rows(self):
        while True:
            self.process_pending_rows()
            time.sleep(self.sleep_per_process_loop)

    def process_pending_rows(self):
        with self._pending_rows_mutex:
            try:
                self.sheet.append_rows(self._pending_rows)
            except Exception as ex:
                print (ex)
                # not sure what to do :/
                return

            self._pending_rows = []

    def emit(self, record):
        row = (record.asctime, record.levelname, record.pathname, record.funcName, record.lineno, record.msg)
        with self._pending_rows_mutex:
            self._pending_rows.append(row)


