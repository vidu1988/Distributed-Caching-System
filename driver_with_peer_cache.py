#!/usr/bin/python

import sys
from node_with_cache import NodeWithCache, Memory, PeerMemory
import traceReader

def main():
    if len(sys.argv) != 4:
        print 'Usage:', sys.argv[0], ' filenameForTrace', ' memorySizeInBlocks', 'peerCacheInBlocks'
        sys.exit(1)
    peerCache = PeerMemory(int(sys.argv[3]), 0)
    n = NodeWithCache(int(sys.argv[2]), peerCache)
    traceRows = traceReader.readFile(sys.argv[1])
    accessCount = {'readCount': set(), 'writeCount': set()}
    accessTime = 100
    doneCount = 0
    for row in traceRows:
        accessTime += 50
        if row.accessType == 'R':
            accessCount['readCount'].update(set(xrange(row.start, row.end+1)))
            n.readBlocksFromDisk(xrange(row.start, row.end+1), accessTime)
        elif row.accessType == 'W':
            accessCount['writeCount'].update(set(xrange(row.start, row.end+1)))
            n.writeBlocksToDisk(xrange(row.start, row.end+1), accessTime)
        else:
            raise Exception('Invalid access type: ' + row.accessType)
        doneCount += 1
        if doneCount%100 == 0:
            sys.stdout.write('Done ' + str((doneCount*100.0)/len(traceRows)) + '%\r')
            sys.stdout.flush()
    # Flush the final writes.
    n.flush()
    print 'Memory access count: ', n._memory._memoryAccessCount
    print 'Peer memory access count: ', peerCache._memoryAccessCount
    print 'Disk read count: ', n._memory._diskReadAccessCount
    print 'Disk write count: ', n._memory._diskWriteAccessCount
    print 'Unique blocks read: ', len(accessCount['readCount'])
    print 'Unique blocks written:', len(accessCount['writeCount'])

if __name__ == "__main__":
        main()
