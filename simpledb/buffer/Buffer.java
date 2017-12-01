package simpledb.buffer;

import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import simpledb.file.*;

/**
 * An individual buffer.
 * A buffer wraps a page and stores information about its status,
 * such as the disk block associated with the page,
 * the number of times the block has been pinned,
 * whether the contents of the page have been modified,
 * and if so, the id of the modifying transaction and
 * the LSN of the corresponding log record.
 * @author Edward Sciore
 * 
 * edits
 * added new attribute timestamps to keep track
 * of pin time
 * @author Mohit Satarkar
 */
public class Buffer {
   //private Page contents = new Page();
   public Page contents = new Page(); //Make page public
   private Block blk = null;
   private int pins = 0;
   private int modifiedBy = -1;  // negative means not modified
   private int logSequenceNumber = -1; // negative means no corresponding log record
   //Edit
   private Queue<Long> timestamps;
   //End Edit
   
   /**
    * Creates a new buffer, wrapping a new 
    * {@link simpledb.file.Page page}.  
    * This constructor is called exclusively by the 
    * class {@link BasicBufferMgr}.   
    * It depends on  the 
    * {@link simpledb.log.LogMgr LogMgr} object 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * 
    * edits 
    * initialize a new timestamps queue for every new buffer created
    * @author Pratyush Gupta
    */
   public Buffer() {
       //Edit
       timestamps = new LinkedList<Long>();
       //End Edit
   }
   
   /**
    * Returns the integer value at the specified offset of the
    * buffer's page.
    * If an integer was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the integer value at that offset
    */
   public int getInt(int offset) {
      return contents.getInt(offset);
   }

   /**
    * Returns the string value at the specified offset of the
    * buffer's page.
    * If a string was not stored at that location,
    * the behavior of the method is unpredictable.
    * @param offset the byte offset of the page
    * @return the string value at that offset
    */
   public String getString(int offset) {
      return contents.getString(offset);
   }

   /**
    * Writes an integer to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * @param offset the byte offset within the page
    * @param val the new integer value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    * 
    * 
    *Edited
    *setInt: Tests if the setInt request is coming from the logFile buffer
    *and updates the logfile accordingly by calling the append function. 
    *(The format for the values to be passed to the append function in LogMgr.java
    *It should be consistent with how they were being written earlier in order for the 
    *Recovery to work properly. So we use
    *
    *Object[] rec = new Object[]{SETINT, txnum, blk.fileName(),blk.number(), offset, val};;
   	 SimpleDB.logMgr().append(rec);)
    *
    *Otherwise, the function does a normal write as applicable for other types of buffers 
    *@author neetishpathak
    */
   public void setInt(int offset, int val, int txnum, int lsn) {
      /*
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setInt(offset, val);
      */
	   
      //Edit
	  int SETINT = 4; //copy from LogRecord.java to properly Update LogFiles
      if(blk.fileName() == SimpleDB.LOG_FILE) {   	  	
    	  		// append to the log buffer
    	  		//Object[] rec = new Object[]{SETINT, val};
   	   		Object[] rec = new Object[]{SETINT, txnum, blk.fileName(),blk.number(), offset, val};;
   	   		SimpleDB.logMgr().append(rec);
   	   		
   	   		//Flush the contents into the logfile
   	   		SimpleDB.logMgr().flush(lsn);
      }else {
    	  		modifiedBy = txnum;
    	  		if (lsn >= 0)
    	  			logSequenceNumber = lsn;
    	  		contents.setInt(offset, val);
      }
      
   }


   /**
    * Writes a string to the specified offset of the
    * buffer's page.
    * This method assumes that the transaction has already
    * written an appropriate log record.
    * A negative lsn value indicates that a log record
    * was not necessary.
    * The buffer saves the id of the transaction
    * and the LSN of the log record.
    * @param offset the byte offset within the page
    * @param val the new string value to be written
    * @param txnum the id of the transaction performing the modification
    * @param lsn the LSN of the corresponding log record
    * 
    * 
    *Edited
    *setInt: Tests if the setString request is coming from the logFile buffer
    *and updates the logfile accordingly by calling the append function. 
    *(The format for the values to be passed to the append function in LogMgr.java
    *It should be consistent with how they were being written earlier in order for the 
    *Recovery to work properly. So we use
    *Object[] rec = new Object[]{SETSTRING, txnum, blk.fileName(),blk.number(), offset, val};;
   	 SimpleDB.logMgr().append(rec);)
    *Otherwise, the function does a normal write as applicable for other types of buffers 
    *@author neetishpathak
    */
   public void setString(int offset, String val, int txnum, int lsn) {
      /*
      modifiedBy = txnum;
      if (lsn >= 0)
	      logSequenceNumber = lsn;
      contents.setString(offset, val);
       */
	   
      //Edit
	  int SETSTRING = 5; //copy from LogRecord.java to properly Update LogFiles
      if(blk.fileName() == SimpleDB.LOG_FILE) {   	  	
    	  		//Also append to the log buffer
    	  		//Object[] rec = new Object[]{SETSTRING, val};
   	   		Object[] rec = new Object[]{SETSTRING, txnum, blk.fileName(),blk.number(), offset, val};
   	   		SimpleDB.logMgr().append(rec);
   	   		
   	   		//Flush the contents into the logfile
   	   		SimpleDB.logMgr().flush(lsn);
      }else {
      modifiedBy = txnum;
	      if (lsn >= 0)
		      logSequenceNumber = lsn;
	      contents.setString(offset, val);
      }
   }
   
   /**
    * Returns a reference to the disk block
    * that the buffer is pinned to.
    * @return a reference to a disk block
    */
   public Block block() {
      return blk;
   }
   
 //Edit
   /**
    * Writes the page to its disk block if the
    * page is dirty (only for Log).
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    * 
    * Edit method to flush the logs
    * @author neetishpathak
    */
   public void flushLog() {
	   contents.write(blk);
   }


   /**
    * Writes the page to its disk block if the
    * page is dirty.
    * The method ensures that the corresponding log
    * record has been written to disk prior to writing
    * the page to disk.
    */
   void flush() {
      if (modifiedBy >= 0) {
         SimpleDB.logMgr().flush(logSequenceNumber);
         contents.write(blk);
         modifiedBy = -1;
      }
   }
   
   /**
    * Increases the buffer's pin count.
    */
   void pin() {
      pins++;
   }

   /**
    * Decreases the buffer's pin count.
    */
   void unpin() {
      pins--;
   }

   /**
    * Returns true if the buffer is currently pinned
    * (that is, if it has a nonzero pin count).
    * @return true if the buffer is pinned
    */
   boolean isPinned() {
      return pins > 0;
   }

   /**
    * Returns true if the buffer is dirty
    * due to a modification by the specified transaction.
    * @param txnum the id of the transaction
    * @return true if the transaction modified the buffer
    */
   boolean isModifiedBy(int txnum) {
      return txnum == modifiedBy;
   }

   /**
    * Reads the contents of the specified block into
    * the buffer's page.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param b a reference to the data block
    */
   void assignToBlock(Block b) {
      flush();
      blk = b;
      contents.read(blk);
      pins = 0;
   }

   /**
    * Initializes the buffer's page according to the specified formatter,
    * and appends the page to the specified file.
    * If the buffer was dirty, then the contents
    * of the previous page are first written to disk.
    * @param filename the name of the file
    * @param fmtr a page formatter, used to initialize the page
    */
   void assignToNew(String filename, PageFormatter fmtr) {
      flush();
      fmtr.format(contents);
      blk = contents.append(filename);
      pins = 0;
   }
  
   /**
    * 
    * @param timestamp
    * edits add the pin time to the buffer
    * only keep the last two pin time
    * @author Bijo Joseph
    */
   //Edit
   public void addTimestamp(Long timestamp) {
       if(timestamps.size() == 2) {
           timestamps.remove();
           timestamps.add(timestamp);
       }
       else {
           
           timestamps.add(timestamp);
       }
   }
   //End Edit
   
   /**
    * edits
    * Clear all timestamps associated with  
    * a buffer when a new block is assigned to it
    * @author Pratuysh Gupta 
    */
   //Edit
   public void clearTimestamps() {
       while(timestamps.size() > 0) {
           timestamps.remove();
       }
   }
   //End Edit
   
   /**
    * 
    * @return timestamps
    * edits
    * Return the timestamps, used during replacement
    * @author Mohit Satarkar
    */
   //Edit
   public List<Long> getTimestamps() {
       List<Long> timestamps = new ArrayList<Long>();
       for(Long timestamp:this.timestamps) {
           timestamps.add(timestamp);
       }
       return timestamps;
   }
   //End Edit
   

   
   /*
    *toString function modification to view the contents of the buffer
    * @author neetishpathak (npathak2)
    * 
    */
   public String toString() {
	   return contents.toString();
	   
   }
   
   /**
    * @return
    * edits
    * Used during testing to get pin time
    */
   public int getPinCount() {
       return pins;
   }
   
}