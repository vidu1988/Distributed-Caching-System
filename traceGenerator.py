#!/usr/bin/python

import sys
import csv
import random


MAX_BLOCKS_IN_READ = 100
MAX_FILE_SIZE = 1000

def main():
    if len(sys.argv) != 2:
        print 'Usage:', sys.argv[0], ' filenameForTrace'
        sys.exit(1)
    traceFile = sys.argv[1]
    with open(traceFile, 'w') as traceCsv:
        traceWriter = csv.writer(traceCsv)
        blocksInFileSystem = xrange(1,1000000)
        # initialize random number with a fix seed.
        random.seed(0)
        numIterations = 10000
        readPercent = 30
        for iteration in xrange(1,numIterations+1):
            fileBlockStart = random.randint(blocksInFileSystem[0], blocksInFileSystem[-1])
            fileBlockEnd = random.randint(fileBlockStart +1, min(blocksInFileSystem[-1], fileBlockStart + MAX_BLOCKS_IN_READ -1))
            fileBlocks = [fileBlockStart, fileBlockEnd]
            if random.randint(0,100) <= readPercent:
                    genTrace(fileBlocks, traceWriter, 'R')
            else:
                    genTrace(fileBlocks, traceWriter, 'W')
            if iteration % 100 == 0:
                print 'Done ', (iteration*100.0)/numIterations, '%'

        #for fileBlocks in otherFiles:
            # Simulate reading and writing data to a file.
            #simulateWriteataToFile(fileBlocks, traceWriter)
            # Simulate random access reads on a file.
            #simulateRandomReadsFile(fileBlocks, traceWriter)
            # Simulate random access writes on a file.
            #simulateRandomReadWritesFile(fileBlocks, traceWriter)


def simulateReadDataFromFile(blockRange, traceWriter):
    blocksToRead = sorted(random.sample(blockRange, 2))
    genTrace(blocksToRead, traceWriter, 'R')

def simulateWriteataToFile(blockRange, traceWriter):
    blocksToRead = sorted(random.sample(blockRange, 2))
    # blocks to write are a subset of blocks to read.
    blocksToWrite = sorted(random.sample(xrange(blocksToRead[0], blocksToRead[1]), 2))
    genTrace(blocksToRead, traceWriter, 'R')
    genTrace(blocksToWrite, traceWriter, 'W')

def simulateRandomReadsFile(blockRange, traceWriter):
    blocksToRead = random.sample(blockRange, min(30, len(blockRange)))
    for b in blocksToRead:
        genTrace([b,b], traceWriter, 'R')

def simulateRandomReadWritesFile(blockRange, traceWriter):
    blocksToRead = random.sample(blockRange, min(30, len(blockRange)))
    for b in blocksToRead:
        accessType = 'W'
        if random.randint(0,1) == 0:
            accessType = 'R'
        genTrace([b,b], traceWriter, accessType)


def genTrace(blocks, traceWriter, accessType):
    blockChunks = splitChunks(blocks)
    for b in blockChunks:
        if (b[-1] - b[0]) >= MAX_BLOCKS_IN_READ:
            raise Exception('Invalid block: ', b, blocks)

        traceWriter.writerow([accessType] + b)

def splitChunks(blocks):
    result = []
    firstValue = blocks[0]
    secondValue = blocks[1]
    while firstValue <= secondValue:
        nextValue = min(firstValue + MAX_BLOCKS_IN_READ - 1, secondValue)
        result.append([firstValue, nextValue])
        firstValue = nextValue + 1
    return result

if __name__ == "__main__":
        main()
