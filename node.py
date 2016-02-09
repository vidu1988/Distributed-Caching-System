
DEBUG= False
MEMORY_READ_TIME = 150
MEMORY_WRITE_TIME = 150
DISK_READ_ACCESS_TIME = 15000
DISK_WRITE_ACCESS_TIME = 15000

class Node:
    def __init__(self, memorySizeInBlocks):
        self._memory = Memory(memorySizeInBlocks)
        self._peerCacheEntries = {}
        self._diskBlockToMemoryEntries = {}
    

    def allocateBlocks(self, blockNumbers, accessTime, isWrite):
            blockNumSet = set(blockNumbers)
            if len(blockNumSet) > self._memory._sizeInBlocks:
                blocks = sorted(blockNumSet)

                raise Exception('Too large request: size: ' + str(len(blocks)) 
                        + ' ' + str(blocks[0]) + ' ' + str(blocks[-1]))
            blocksNotInMem = []
            requestBlocks = []
            for b in blockNumbers:
                    if b in self._diskBlockToMemoryEntries:
                            requestBlocks.append(self._diskBlockToMemoryEntries[b])
                    else:
                            blocksNotInMem.append(b)

            while blocksNotInMem:
                    # try to allocate memory for blocks that are not in memory.
                    memoryBlocks = self._memory._findBlocks(len(blocksNotInMem), requestBlocks)
                    # memory can return blocks that are already in memory.
                    debugLog(blocksNotInMem, memoryBlocks)
                    for b in memoryBlocks:
                            if not b.isFree:
                                    if b.diskBlock in blockNumSet:
                                            # this block is part of the request and already in memory,
                                            # so no need to free it.
                                            b.accessTime = accessTime
                                            if isWrite:
                                                b.isDirty = True
                                            continue
                                    else:
                                            # this block is not free, free it. 
                                            del self._diskBlockToMemoryEntries[b.diskBlock]
                                            self._memory.freeBlock(b)
                            self._memory.allocateBlock(b, blocksNotInMem.pop())
                            self._diskBlockToMemoryEntries[b.diskBlock] = b
                            self._memory._diskReadAccessCount += 1
                            requestBlocks.append(b)
            return requestBlocks

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
                  raise 'Attempt to allocate a used memory block', block
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
