#!/usr/bin/python

import sys
import csv


class TraceRow:
    def __init__(self, accessType, start, end):
        self.accessType = accessType
        self.start = start
        self.end = end

def readFile(traceFilename):
    result = []
    with open(traceFilename, 'r') as traceCsv:
        traceReader = csv.reader(traceCsv)
        for row in traceReader:
            result.append(TraceRow(row[0], int(row[1]), int(row[2])))
    return result



