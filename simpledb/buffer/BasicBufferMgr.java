package simpledb.buffer;

import simpledb.file.*;
import java.util.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   //Edits
   private static final HashMap<Block, Buffer> bufferPoolMap = new LinkedHashMap<Block, Buffer>();
   private int bufferPoolSize;
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      bufferPoolSize = numbuffs; //Edit
      /*
      for (int i=0; i<numbuffs; i++)
         bufferpool[i] = new Buffer();
      */
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
       /*
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
      */
       //Edits
       for(Buffer buffer :bufferPoolMap.values()) {
           if(buffer.isModifiedBy(txnum)) {
             buffer.flush();            
           }
       }
       //Edits
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         /*
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         */
         //Edit
         
         //Remove the buffer and clear the timestamps
         bufferPoolMap.remove(buff.block());
         buff.clearTimestamps();
         
         buff.assignToBlock(blk);
         if (!buff.isPinned())
             numAvailable--;
          buff.pin();
          
          //Add pin time to buffer
          buff.addTimestamp(System.currentTimeMillis());
          
          //Now put BUffer into buffer pool
          bufferPoolMap.put(blk, buff);
          return buff;
         //End Edit
      }
      else {
        if (!buff.isPinned())
          numAvailable--;
        
        buff.pin();
        Buffer buffer = bufferPoolMap.get(blk);
        bufferPoolMap.remove(buff.block());
        buffer.addTimestamp(System.currentTimeMillis());
        bufferPoolMap.put(blk, buffer);
        return buff;
      }
      
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) throws BufferAbortException{
      /*Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      return buff;
      */
      
      
      //Edit
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      //Remove the buffer and clear the timestamps
      bufferPoolMap.remove(buff.block());
      buff.clearTimestamps();
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      //Add pin time to buffer
      buff.addTimestamp(System.currentTimeMillis());
      //Now put BUffer into buffer pool
      bufferPoolMap.put(buff.block(), buff);
      return buff;
     //End Edit
   }
   
   
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
   private Buffer findExistingBuffer(Block blk) {
       /*
      for (Buffer buff : bufferpool) {
         Block b = buff.block();
         if (b != null && b.equals(blk))
            return buff;
      }
      return null;
      */
     //Edit
     //Return the block requested, if it is present in the
     //buffer pool.
     if (!bufferPoolMap.containsKey(blk)) {
         return null;
     }
     else {
         Buffer buffer = bufferPoolMap.get(blk);         
         return buffer;
     }
   }
   
   private Buffer chooseUnpinnedBuffer() throws BufferAbortException{
       
      //To Do implement LRU(k) and LRU here
      /*
      for (Buffer buff : bufferpool)
         if (!buff.isPinned())
         return buff;
      return null;
      */
       
      //Edits
      if(numAvailable > 0) {
          if(bufferPoolMap.size() < bufferPoolSize) {
              Buffer buffer = new Buffer();
              return buffer;
          }
          else {
            Buffer buffer = LRU2();
            return buffer;
          }
      }
      else {
          throw new BufferAbortException();
      }
   }
   
   public Buffer LRU2() throws BufferAbortException{
       int numInfinity = 0;
       int bufferInfoIndex = -1;
       List<Buffer> unpinnedBufferList = new ArrayList<Buffer>();
       List<Buffer> unpinnedBufferListWithLRU2Inf = new ArrayList<Buffer>();
       for(Map.Entry<Block, Buffer> b : bufferPoolMap.entrySet()) {
           Buffer buffer = b.getValue();
     
           if(!buffer.isPinned()) {
               unpinnedBufferList.add(buffer);
           }
       }
       
       if(unpinnedBufferList.size() > 0) {
           for(int i = 0; i < unpinnedBufferList.size(); i++) {
               Buffer buffer = unpinnedBufferList.get(i);
               List<Long> timestamps = buffer.getTimestamps();
               //System.out.println(timestamps.toString());
               if(timestamps.size() < 2) {
                   numInfinity++;
                   bufferInfoIndex = i;
                   unpinnedBufferListWithLRU2Inf.add(buffer);
               }
           }
           //Chose buffer by using LRU2
           if(numInfinity == 0) {
               long maxDistance = -1;
               long currentTime = System.currentTimeMillis();
               int indexOfBufferToReplace = 0;
               
               //Get the buffer that has the maximum LRU2 distance
               //At the end of for loop we will have the buffer
               //at maximum LRU2 distance
               for(int i = 0; i < unpinnedBufferList.size(); i++) {
                   Buffer buffer = unpinnedBufferList.get(i);
                   List<Long> timestamps = buffer.getTimestamps();
                   long tmpMaxDistance = currentTime - timestamps.get(0);
                   
                   if(tmpMaxDistance > maxDistance) {
                       maxDistance = tmpMaxDistance;
                       indexOfBufferToReplace = i;
                   }
               }
               
               Buffer buffer = unpinnedBufferList.get(indexOfBufferToReplace);          
               return buffer;
           }
           
           //Get the buffer index, as it occurs only once
           //this is the buffer to replace
           else if(numInfinity == 1) {
               Buffer buffer = unpinnedBufferList.get(bufferInfoIndex);
               return buffer;
           }
           
           else {
               Buffer buffer = LRU(unpinnedBufferListWithLRU2Inf);
               return buffer;
           }
       }
       else {
           throw new BufferAbortException();
       }
   }
   
   /*
    * @bufferInfoIndex - index of buffer to be replaced
    * 
    */
   private Buffer LRU(List<Buffer> unpinnedBufferListWithLRU2Inf) {
       int bufferInfoIndex = -1;
       long maxLeastRecentTimeDifference = -1;
       //Chose the buffer that has the lowest timestamp
       long currentTime = System.currentTimeMillis();
 
       for(int i = 0;i < unpinnedBufferListWithLRU2Inf.size(); i++) {
           Buffer buffer = unpinnedBufferListWithLRU2Inf.get(i);
           List<Long> timestamps = buffer.getTimestamps();
           long tmpLeastRecentTime = timestamps.get(timestamps.size() - 1);
           
           //keep track of the least time and the bufferInfoIndex
          // System.out.println("Index " + i + "Time Difference " + (currentTime - tmpLeastRecentTime));
           if(maxLeastRecentTimeDifference < currentTime - tmpLeastRecentTime) {
               maxLeastRecentTimeDifference = currentTime - tmpLeastRecentTime;
               bufferInfoIndex = i;
               //System.out.println("Buffer Index "+bufferInfoIndex);
           }
       }
       Buffer buffer = unpinnedBufferListWithLRU2Inf.get(bufferInfoIndex);       
       return buffer;
   }
   
   /*
    * Return the mapping of blk to buffer
    * Used during testing to check if mapping
    * is correct
    */
   public Map<Block, Buffer> getBufferPoolMap() {
       Map<Block, Buffer> bufferPool = new HashMap<Block,Buffer>();
       for (Map.Entry<Block, Buffer> e : bufferPoolMap.entrySet()) {
           bufferPool.put(e.getKey(), e.getValue());
       } 
       return bufferPool;
   }

   public void clearBufferPoolMap() {
       bufferPoolMap.clear();
   }

   //Edit
   /*
    * Determines whether the map has a mapping
    * from the block to some buffer 
    * @param blk the block to use as key
    * @return true if there is a mapping
    */
   public boolean containsMapping(Block blk) {
       return bufferPoolMap.containsKey(blk);
   }
   
   /*
    * Returns the buffer the map maps the
    * specified block to
    * @param blk the block to use as key
    * @return the buffer mapped to it otherwise null;
    */
   public Buffer getMapping(Block blk) {
       if(containsMapping(blk)) {
         return bufferPoolMap.get(blk);
       }
       return null;
   }
   //End Edit
}
