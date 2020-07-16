# csmlog

[![Build Status](https://csm10495.visualstudio.com/csmlog/_apis/build/status/csmlog-CI?branchName=master)](https://csm10495.visualstudio.com/csmlog/_build/latest?definitionId=4?branchName=master)

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
