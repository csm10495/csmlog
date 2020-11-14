# csmlog

[![Build Status](https://github.com/csm10495/csmlog/workflows/Release/badge.svg)](https://github.com/csm10495/csmlog/actions)

Package to setup a python logger the way I like to use it.

- By default logs to files per logger and one for the overall project
- Sets a master logger with sub loggers per file (obtained via getLogger())

## Usage

```
from csmlog import setup, getLogger
setup("appName") # call setup once whenever you would like to set the output location for future loggers
logger = getLogger(__file__)

# logger is a Python logger... feel free to use it.
# You should see logs in %APPDATA% on Windows and /var/log or ~/log on Linux/Mac
```

## Google Sheets Logging
`setup()` has an optional parameter: `googleSheetShareEmail`. If it is given, it should be an email address to share a Google Sheets worksheet of logs.

In order to use this, you must have a Google Service Account or User Account's credentials (in JSON form) in ~/.gcreds.json

Internally the gspread module is used for Google Sheets communication/authentication. See https://gspread.readthedocs.io/en/latest/oauth2.html for more information on getting credentials. The only thing different from their instructions is that for csmlog, the JSON credentials should be stored in ~/.gcreds.json.

Note that logs may be delayed due to rate limiting, etc. If you are logging *a lot*, it may not be a good idea to enable this feature.

## Installation
```
pip install csmlog
```