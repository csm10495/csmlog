"""
This file is part of csmlog. Python logger setup... the way I like it.
MIT License (2019) - Charles Machalow
"""

import subprocess
import threading
import time


class LoggedSystemCall(object):
    """helpful to do a system call and log the output"""

    def __init__(self, logger):
        self.logger = logger

    def _logIo(self, process):
        """sends output to the logger (one line at a time) and returns the output"""
        savedOutput = ""
        while True:
            output = process.stdout.readline().decode()
            if output:
                savedOutput += output
                self.logger.debug("<CMD OUTPUT>: %s" % output.rstrip("\n"))
            else:
                break
        return savedOutput

    def check_output(self, cmd, shell=False):
        """similiar to subprocess.check_output but sends all output to the logger"""
        self.logger.debug("About to call: %s" % cmd)
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=shell
        )
        output = ""
        while process.poll() is None:
            output += self._logIo(process)
            time.sleep(0.00001)  # yield a bit

        self.logger.debug(".. Exit Code: %d" % process.returncode)

        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd, output)
        return output

    def call(self, cmd, shell=False):
        """similiar to subprocess.call but sends all output to the logger"""
        try:
            self.check_output(cmd, shell=shell)
            return 0
        except subprocess.CalledProcessError as ex:
            return ex.returncode
