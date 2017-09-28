#!/usr/bin/python

# Copyright (C) 2016 CEA/DAM
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the CeCILL License.
#
# The fact that you are presently reading this means that you have had
# knowledge of the CeCILL license (http://www.cecill.info) and that you
# accept its terms.

"""
Robinhood v3 API Nagios plugin

This script query a robinhood v3 database throught the web API.
It returns the status in nagios format with optionnal perf data.
"""

import requests
import json
import sys
import time,datetime
from optparse import OptionParser

class Convert():
    """
    Simple class to convert size
    """
    units = ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')

    def h2b(self, string):
        """
        Convert human readable size to bytes
        """
        num = float(string[:-1])
        p = {self.units[0]:1}
        for i, s in enumerate(self.units[1:]):
            p[s] = 1024 ** (i+1)
        return int(num * p[string[-1:].upper()])

    def b2h(self, n):
        """
        Convert bytes to human readable size
        """
        p = {}
        for i, s in enumerate(self.units[1:]):
            p[s] = 1024 ** (i+1)
        for unit in reversed(self.units[1:]):
            if int(n) >= p[unit]:
                val = float(n) / p[unit]
                return "%f%s" % (val, unit)
        return "%dB" % n

class RobinHood():
    args = {}
    options = {}

    def __init__(self):
        parser = OptionParser(usage="usage: %prog [options] server",
                version="%prog 1.0")
        parser.add_option("-p", "--perf",
                action="store_true",
                dest="perf",
                default=False,
                help="Add perfstats")
        parser.add_option("-q", "--query",
                action="store",
                dest="query",
                default="scan",
                help="scan or user_size",)
        parser.add_option("-w", "--warning",
                action="store",
                dest="warning",
                default="100T",
                help="",)
        parser.add_option("-c", "--critical",
                action="store",
                dest="critical",
                default="1P",
                help="",)
        (self.options, self.args) = parser.parse_args()

        if len(self.args) != 1:
            parser.error("wrong number of arguments")
            sys.exit(2)

    def getData(self,request):
        try:
            r = requests.get("http://%s/robinhood/api/index.php?request=%s" % (self.args[0], request))
            return json.loads(r.text)
        except:
            print "Can't retrieve data: ",sys.exc_info()
            sys.exit(2)


    def runQuery(self):
        code = 0
        text = ""

        if self.options.query == "scan":
            data = self.getData("native/vars")
            nextscan = int(data['LastScanStartTime']) + int(data['ScanInterval'])
            if int(time.time()) > nextscan+600 and int(time.time()) < nextscan+36000:
                text = text + "WARNING: Scan is lightly overdue"
                code = 1
            elif int(time.time()) > nextscan+36000:
                text = text + "CRITICAL: Scan is overdue"
                code = 2
            else:
                text = text + "OK: On time"

            text = text + " / Nextscan: " + datetime.datetime.utcfromtimestamp(nextscan).strftime('%Y-%m-%dT %H:%M:%SZ')

            if self.options.perf:
                text = text + " | nextscan=%d, overdue=%d " % (nextscan, int(time.time()) - nextscan)

        elif self.options.query == "user_size":
            data = self.getData("native/acct/uid.group")
            critical = Convert().h2b(self.options.critical)
            warning = Convert().h2b(self.options.warning)
            warn = {}
            crit = {}
            perf = []

            for item in data:
                if int(item["size"])>warning and int(item["size"])<critical:
                    warn[item["uid"]]=Convert().b2h(item["size"])
                elif int(item["size"])>critical:
                    crit[item["uid"]]=Convert().b2h(item["size"])
                if self.options.perf:
                    perf.append("%s=%s" % (item["uid"], item["size"]))

            if (len(warn)>0):
                msg = "WARNING: "
                code = 1
                for k in warn:
                    msg = msg + k + "=" + warn[k] + " "
                text = text + msg + " "

            if (len(crit)>0):
                msg = "CRITICAL: "
                code = 2
                for k in crit:
                    msg = msg + k + "=" + crit[k] + " "
                text = msg + text

            if len(warn)==0 and len(crit)==0:
                text = "OK"

            if self.options.perf:
                perf = " | %s " % (", ".join(perf))
                text = text + perf

        return (text,code)


Inst = RobinHood()
result = Inst.runQuery()

print result[0]
sys.exit(result[1])
