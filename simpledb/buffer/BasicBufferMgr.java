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
   private static final HashMap<Block, BufferInfo> bufferPoolMap = new LinkedHashMap<Block, BufferInfo>();
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
       for(BufferInfo bufferInfo :bufferPoolMap.values()) {
           Buffer buffer = bufferInfo.getBuffer();
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
         bufferPoolMap.remove(buff.block());
         buff.assignToBlock(blk);
         if (!buff.isPinned())
             numAvailable--;
          buff.pin();
          
          //Create a new Buffer Info
          BufferInfo bufferInfo = new BufferInfo();
          bufferInfo.addBufferToBufferInfo(buff);
          bufferInfo.addTimestamp(System.currentTimeMillis());
          
          //Now put BUfferInfo into buffer pool
          bufferPoolMap.put(blk, bufferInfo);
          return buff;
         //End Edit
      }
      else {
        if (!buff.isPinned())
          numAvailable--;
        
        buff.pin();
        BufferInfo bufferInfo = bufferPoolMap.get(blk);
        bufferInfo.addTimestamp(System.currentTimeMillis());
        bufferPoolMap.put(blk, bufferInfo);
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
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      return buff;
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
         BufferInfo bufferInfo = bufferPoolMap.get(blk);
         Buffer buffer = bufferInfo.getBuffer();
         
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
       List<BufferInfo> unpinnedBufferInfoList = new ArrayList<BufferInfo>();
       for(Map.Entry<Block, BufferInfo> b : bufferPoolMap.entrySet()) {
           BufferInfo bufferInfo = b.getValue();
           Buffer buffer = bufferInfo.getBuffer();
           if(!buffer.isPinned()) {
               unpinnedBufferInfoList.add(bufferInfo);
           }
       }
       
       if(unpinnedBufferInfoList.size() > 0) {
           for(int i = 0; i < unpinnedBufferInfoList.size(); i++) {
               BufferInfo bufferInfo = unpinnedBufferInfoList.get(i);
               List<Long> timestamps = bufferInfo.getTimestamps();
               //System.out.println(timestamps.toString());
               if(timestamps.size() < 2) {
                   numInfinity++;
                   bufferInfoIndex = i;
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
               for(int i = 0; i < unpinnedBufferInfoList.size(); i++) {
                   BufferInfo bufferInfo = unpinnedBufferInfoList.get(i);
                   List<Long> timestamps = bufferInfo.getTimestamps();
                   long tmpMaxDistance = currentTime - timestamps.get(0);
                   
                   if(tmpMaxDistance > maxDistance) {
                       maxDistance = tmpMaxDistance;
                       indexOfBufferToReplace = i;
                   }
               }
               
               BufferInfo bufferInfo = unpinnedBufferInfoList.get(indexOfBufferToReplace);
               Buffer buffer = bufferInfo.getBuffer();
               
               return buffer;
           }
           //Get the bufferInfo index, as it occurs only once
           //this is the buffer to replace
           else if(numInfinity == 1) {
               BufferInfo bufferInfo = unpinnedBufferInfoList.get(bufferInfoIndex);
               Buffer buffer = bufferInfo.getBuffer();
               return buffer;
           }
           else {
               Buffer buffer = LRU(unpinnedBufferInfoList);
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
   private Buffer LRU(List<BufferInfo> unpinnedBufferInfoList) {
       int bufferInfoIndex = -1;
       long maxLeastRecentTimeDifference = -1;
       //Chose the buffer that has the lowest timestamp
       long currentTime = System.currentTimeMillis();
       for(int i = 0;i < unpinnedBufferInfoList.size(); i++) {
           BufferInfo bufferInfo = unpinnedBufferInfoList.get(i);
           List<Long> timestamps = bufferInfo.getTimestamps();
           long tmpLeastRecentTime = timestamps.get(timestamps.size() - 1);
           
           //keep track of the least time and the bufferInfoIndex
          // System.out.println("Index " + i + "Time Difference " + (currentTime - tmpLeastRecentTime));
           if(maxLeastRecentTimeDifference < currentTime - tmpLeastRecentTime) {
               maxLeastRecentTimeDifference = currentTime - tmpLeastRecentTime;
               bufferInfoIndex = i;
               //System.out.println("Buffer Index "+bufferInfoIndex);
           }
       }
       BufferInfo bufferInfo = unpinnedBufferInfoList.get(bufferInfoIndex);
       Buffer buffer = bufferInfo.getBuffer();
       
       return buffer;
   }
   
   /*
    * Return the mapping of blk to buffer
    * Used during testing to check if mapping
    * is correct
    */
   public Map<Block, Buffer> getBufferPoolMap() {
       Map<Block, Buffer> bufferPool = new HashMap<Block,Buffer>();
       for (Map.Entry<Block, BufferInfo> e : bufferPoolMap.entrySet()) {
           bufferPool.put(e.getKey(), e.getValue().getBuffer());
       } 
       return bufferPool;
   }

   public void clearBufferPoolMap() {
       bufferPoolMap.clear();
   }

   /*
    * Class that has the buffer
    * and also the timestamp of last and
    * second last arrival
    */
   public class BufferInfo {
       private Buffer buffer;
       private Queue<Long> timestamps;
       
       private BufferInfo() {
           this.buffer = null;
           timestamps = new LinkedList<Long>();
       }
       private void addBufferToBufferInfo(Buffer buffer) {
           this.buffer = buffer;
       }
       private void addTimestamp(Long timestamp) {
           if(timestamps.size() == 2) {
               timestamps.remove();
               timestamps.add(timestamp);
           }
           else {
               
               timestamps.add(timestamp);
           }
       }
       private Buffer getBuffer() {
           return this.buffer;
       }
       private List<Long> getTimestamps() {
           List<Long> timestamps = new ArrayList<Long>();
           for(Long timestamp:this.timestamps) {
               timestamps.add(timestamp);
           }
           return timestamps;
       }
   }
}
