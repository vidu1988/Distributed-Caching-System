
import random

DEBUG= False
MEMORY_READ_TIME = 150
MEMORY_WRITE_TIME = 150
DISK_READ_ACCESS_TIME = 15000
DISK_WRITE_ACCESS_TIME = 15000

class NodeWithCache:
    def __init__(self, memorySizeInBlocks, peerMemory):
        self._memory = Memory(memorySizeInBlocks)
        self._peerCacheEntries = set()
        self._diskBlockToMemoryEntries = {}
        self._peerCache = peerMemory


    def allocateBlocks(self, blockNumbers, accessTime, isWrite):
            blockNumSet = set(blockNumbers)
            if len(blockNumSet) > self._memory._sizeInBlocks:
                blocks = sorted(blockNumSet)

                raise Exception('Too large request: size: ' + str(len(blocks))
                        + ' ' + str(blocks[0]) + ' ' + str(blocks[-1]))
            blocksNotInMem = []
            requestBlocks = []
            blocksInPeerCache = []
            for b in blockNumbers:
                    if b in self._diskBlockToMemoryEntries:
                            requestBlocks.append(self._diskBlockToMemoryEntries[b])
                    elif b in self._peerCacheEntries:
                        blocksInPeerCache.append(b)
                    else:
                        blocksNotInMem.append(b)
            if blocksInPeerCache:
                # find space for blocks that need to be fetched from the peer cache in the local
                # memory.
                memoryBlocks = self._memory._findBlocks(len(blocksInPeerCache), requestBlocks)
                for b in memoryBlocks:
                    if not b.isFree:
                        if b.diskBlock in blockNumSet:
                            raise Exception('Invalid block: ', b)
                        else:
                            self._freeBlockWithPeerCache(b)
                    diskBlock = blocksInPeerCache.pop()
                    peerBlock = self._peerCache.fetchBlock(diskBlock, accessTime)
                    if peerBlock:
                        self._memory.allocateBlock(b, diskBlock)
                        b.accessTime = accessTime
                        debugLog('Found in peer cache:', b)
                        if isWrite:
                            # in case of write remove the block from the peer cache.
                            self._peerCache.removeBlock(diskBlock)
                            self._peerCacheEntries.remove(diskBlock)
                            b.isDirty = True
                        self._diskBlockToMemoryEntries[b.diskBlock] = b
                        requestBlocks.append(b)
                    else:
                        # not found in peer cache, try to look it up on disk.
                        blocksNotInMem.append(diskBlock)

            while blocksNotInMem:
                    # try to allocate memory for blocks that are not in memory.
                    memoryBlocks = self._memory._findBlocks(len(blocksNotInMem), requestBlocks)
                    # memory can return blocks that are already in memory.
                    debugLog(blocksNotInMem, memoryBlocks)
                    for b in memoryBlocks:
                            if not b.isFree:
                                    if b.diskBlock in blockNumSet:
                                        raise Exception('Invalid block: ', b)
                                    else:
                                        # this block is not free, free it.
                                        self._freeBlockWithPeerCache(b)
                            self._memory.allocateBlock(b, blocksNotInMem.pop())
                            self._diskBlockToMemoryEntries[b.diskBlock] = b
                            self._memory._diskReadAccessCount += 1
                            requestBlocks.append(b)
            return requestBlocks

    def _freeBlockWithPeerCache(self, b):
        del self._diskBlockToMemoryEntries[b.diskBlock]
        if b.diskBlock not in self._peerCacheEntries:
            # this block is about to be evicted from cache.
                self._peerCacheEntries.add(b.diskBlock)
                self._peerCache.storeBlock(b.diskBlock, b.accessTime)

        self._memory.freeBlock(b)



    def readBlocksFromDisk(self, blockNumbers, accessTime):
            blocksToAllocate = self.allocateBlocks(blockNumbers, accessTime, False)
            self._memory.readBlocks(blocksToAllocate, accessTime)


    def writeBlocksToDisk(self, blockNumbers, accessTime):
            blocksToAllocate = self.allocateBlocks(blockNumbers, accessTime, True)
            self._memory.writeBlocks(blocksToAllocate, accessTime)

    def flush(self):
        for _, memBlock in self._diskBlockToMemoryEntries.items():
                self._memory.freeBlock(memBlock)


def debugLog(*msg):
        if DEBUG:
                print map(str, msg)


class Memory:
    def __init__(self, sizeInBlocks):
        self._sizeInBlocks = sizeInBlocks
        self._allocatedSize = 0
        self._allocatedEntries = []
        self._memoryAccessCount = 0
        self._diskReadAccessCount = 0
        self._diskWriteAccessCount = 0
        self._freeBlocks = set()
        self._usedBlocks = set()
        for i in xrange(sizeInBlocks):
            memoryBlock = MemoryBlock(i, True, 0, False)
            self._freeBlocks.add(memoryBlock)

    def allocateBlock(self, block, diskBlock):
          debugLog('Allocating block: ',  block.num)
          if not block.isFree:
                  raise 'Attempt to allocate a non free block ', block
          if block.diskBlock != -1:
                  raise Exception('Attempt to allocate a used memory block', block)
          block.isFree = False
          block.diskBlock = diskBlock
          self._freeBlocks.remove(block)
          self._usedBlocks.add(block)



    def freeBlock(self, block):
          debugLog('Freeing block: ',  block.num)
          if block.isDirty:
            debugLog('Writing block: ', block.num, 'to disk block: ', block.diskBlock)
            self._diskWriteAccessCount += 1
          block.isFree = True
          block.isDirty = False
          block.diskBlock = -1
          self._memoryAccessCount += 1
          self._freeBlocks.add(block)
          self._usedBlocks.remove(block)

    def writeBlocks(self, blocks, accessTime):
            for b in blocks:
                    if b.isFree:
                            raise 'Attempt to write to a free block ', b
                    if b.diskBlock == -1:
                            raise 'Attempt to read invalid disk block'
                    b.isDirty = True
                    b.accessTime = accessTime
                    self._memoryAccessCount += 1

    def readBlocks(self, blocks, accessTime):
            for b in blocks:
                    if b.isFree:
                            raise 'Attempt to read a free block ', b
                    if b.diskBlock == -1:
                            raise 'Attempt to read invalid disk block'
                    b.accessTime = accessTime
                    self._memoryAccessCount += 1

    def _findBlocks(self, size, excludedBlocks):
        # there is at least one access to memory to find the blocks.
        self._memoryAccessCount += 1
        allocatedBlocks = list(self._freeBlocks)[:size]
        if len(allocatedBlocks) >= size:
                return allocatedBlocks

        usedBlocks = self._usedBlocks.difference(set(excludedBlocks))

        # Sort blocks to be FIFO:
        sortedByTimeBlocks = sorted(usedBlocks, cmp=Memory.fifoSort)

        for block in sortedByTimeBlocks:
            allocatedBlocks.append(block)
            if len(allocatedBlocks) >= size:
                return allocatedBlocks
        raise Exception('Unable to return allocated blocks', allocatedBlocks, size, self._freeBlocks, self._usedBlocks)

    @staticmethod
    def fifoSort(x,y):
        return Memory.sgn(x.accessTime - y.accessTime)

    @staticmethod
    def sgn(a):
       if a == 0:
           return 0
       return  1 if (a>0) else  -1



class MemoryBlock:

    def __init__(self, number, isFree, accessTime, isDirty):
        self.num = number
        self.isFree = isFree
        self.accessTime = accessTime
        self.isDirty = isDirty
        self.diskBlock = -1
    def __repr__(self):
        return str(map(str, [self.num, self.isFree, self.accessTime, self.isDirty, self.diskBlock]))


class PeerMemory:
    def __init__(self, sizeInBlocks, dropProbability):
        self._dropProb = dropProbability
        self._sizeInBlocks = sizeInBlocks
        self._allocatedSize = 0
        self._allocatedEntries = []
        self._memoryAccessCount = 0
        self._freeBlocks = set()
        for i in xrange(sizeInBlocks):
            memoryBlock = MemoryBlock(i, True, 0, False)
            self._freeBlocks.add(memoryBlock)
        self._usedBlocks = {}

    def storeBlock(self, block, accessTime):
        if block in self._usedBlocks:
            self._usedBlocks[block].accessTime = accessTime
            return
        if self._shouldDrop():
            return False
        if len(self._freeBlocks) == 0:
            lowestTimeBlock = self.lowestTimeBlock()
            if lowestTimeBlock.accessTime > accessTime:
                return False
            self.removeBlock(lowestTimeBlock.diskBlock)
        memBlock = self._freeBlocks.pop()
        memBlock.diskBlock = block
        memBlock.isFree = False
        memBlock.isDirty = False
        memBlock.accessTime = accessTime
        self._usedBlocks[block] = memBlock
        self._memoryAccessCount += 1

    def updateAccessTime(self, block, accessTime):
        if self._shouldDrop() or block not in self._usedBlocks:
            return
        self._usedBlocks[block].accessTime = accessTime
    def fetchBlock(self, block, accessTime):
        if self._shouldDrop() or block not in self._usedBlocks:
            return None
        memBlock = self._usedBlocks[block]
        memBlock.accessTime = accessTime
        self._memoryAccessCount += 1
        return memBlock

    def removeBlock(self, block):
        if self._shouldDrop():
            return False
        self._memoryAccessCount += 1
        if block in self._usedBlocks:
            memBlock = self._usedBlocks[block]
            memBlock.isFree = True
            memBlock.diskBlock = -1
            self._freeBlocks.add(memBlock)
            del self._usedBlocks[block]
        return True
    def lowestTimeBlock(self):
        return sorted(self._usedBlocks.iteritems())[0][1]



    def _shouldDrop(self):
        return False and random.randint(0, 100) <= self._dropProb



class Disk:

    def __init__(self):
        self._diskBlocks = 1e6
        self._accessTimePerBlock = 1

    def checkAccess(self, start, end):
        if(self.end > self._diskBlocks or start > end) :
            raise 'Illegal argument :' + str(start) + " - " + str(end)
    def readToMemory(self, start, end, memoryBlock):
        self.checkAccess(start, end)
        memoryWriteTime = (end -start+1) * self._accessTimePerBlock
        memoryWriteTime += memoryBlock.writeTime(start, end)
        return memoryWriteTime
