package simpledb.log;

import simpledb.server.SimpleDB;
import simpledb.buffer.Buffer;
import simpledb.buffer.PageFormatter;
import simpledb.file.*;
import static simpledb.file.Page.*;
import java.util.*;

/**
 * The low-level log manager.
 * This log manager is responsible for writing log records
 * into a log file.
 * A log record can be any sequence of integer and string values.
 * The log manager does not understand the meaning of these
 * values, which are written and read by the
 * {@link simpledb.tx.recovery.RecoveryMgr recovery manager}.
 * @author Edward Sciore
 */
public class LogMgr implements Iterable<BasicLogRecord> {
   /**
    * The location where the pointer to the last integer in the page is.
    * A value of 0 means that the pointer is the first value in the page.
    */
   public static final int LAST_POS = 0;

   private String logfile;
   private Page mypage = new Page();
   private Block currentblk;
   private int currentpos;
  
   // Added LogManager's Buffer
   public static Buffer logBuffer;
   
   /**
    * Creates the manager for the specified log file.
    * If the log file does not yet exist, it is created
    * with an empty first block.
    * This constructor depends on a {@link FileMgr} object
    * that it gets from the method
    * {@link simpledb.server.SimpleDB#fileMgr()}.
    * That object is created during system initialization.
    * Thus this constructor cannot be called until
    * {@link simpledb.server.SimpleDB#initFileMgr(String)}
    * is called first.
    * @param logfile the name of the log file
    * 
    * 
    * Edit
    * A new buffer now should be allocated when LogMgr method is invoked
    * this buffer reference is used to do read/write operations
    * 
    * @author neetishpathak
    */
   public LogMgr(String logfile) {
      this.logfile = logfile;
      int logsize = SimpleDB.fileMgr().size(logfile);
      if (logsize == 0) {
         appendNewBlock();
         System.out.println("Log File Assigned New Buffer");
      }
   else {
         /*
         currentblk = new Block(logfile, logsize-1);
         mypage.read(currentblk);
         currentpos = getLastRecordPosition() + INT_SIZE;
         */
	   
         //Edit
         currentblk = new Block(logfile, logsize-1);
         logBuffer = SimpleDB.bufferMgr().pin(currentblk);
         currentpos = getLastRecordPosition() + INT_SIZE;
         System.out.println("Log File Assigned a Buffer");
         
      }
   }

   /**
    * Ensures that the log records corresponding to the
    * specified LSN has been written to disk.
    * All earlier log records will also be written to disk.
    * @param lsn the LSN of a log record
    * 
    * Edit 
    * Since flush does not explicitly takes lsn and it is internally managed by the 
    * logmgr about the location where the log needs to be updated, we can ignore it 
    * for the time being 
    * 
    * @author neetishpathak
    */
   public void flush(int lsn) { 
      //if (lsn >= currentLSN())
	   //Edit
	   flush();
   }

   /**
    * Returns an iterator for the log records,
    * which will be returned in reverse order starting with the most recent.
    * @see java.lang.Iterable#iterator()
    */
   public synchronized Iterator<BasicLogRecord> iterator() {
      flush();
      return new LogIterator(currentblk);
   }

   /**
    * Appends a log record to the file.
    * The record contains an arbitrary array of strings and integers.
    * The method also writes an integer to the end of each log record whose value
    * is the offset of the corresponding integer for the previous log record.
    * These integers allow log records to be read in reverse order.
    * @param rec the list of values
    * @return the LSN of the final value
    *
    *Edit: Append makes use of bufferMgr's unpin to manage the current buffer
    *appendNewblock will assign a new block from the buffer pool
    *
    *@author neetish pathak
    */
   public synchronized int append(Object[] rec) {
      /*int recsize = INT_SIZE;  // 4 bytes for the integer that points to the previous log record
      for (Object obj : rec)
         recsize += size(obj);
      if (currentpos + recsize >= BLOCK_SIZE){ // the log record doesn't fit,
         flush();        // so move to the next block.
         appendNewBlock();
      }
      for (Object obj : rec)
         appendVal(obj);
      finalizeRecord();
      return currentLSN();
      */
	   
      //Edit
      int recsize = INT_SIZE;  // 4 bytes for the integer that points to the previous log record
      for (Object obj : rec)
         recsize += size(obj);
      if (currentpos + recsize >= BLOCK_SIZE){ // the log record doesn't fit,
         flush();        // so move to the next block.
         SimpleDB.bufferMgr().unpin(logBuffer); // Unpin the log Buffer
         appendNewBlock();
      }
      for (Object obj : rec)
         appendVal(obj);
      finalizeRecord();
      return currentLSN();
   }

   public void printLogPageBuffer() {
	   if(logBuffer != null) {
		   System.out.println(logBuffer.toString());
		   return;
	   }
	   System.out.println("LogPage is not Assigned a buffer");
   }
   
   /**
    * Adds the specified value to the page at the position denoted by
    * currentpos.  Then increments currentpos by the size of the value.
    * @param val the integer or string to be added to the page
    * 
    * Edit
    * appendVal now makes use of setString and setInt by retrieving the 
    * page of the current buffer
    */
   private void appendVal(Object val) {
      /*if (val instanceof String)
         mypage.setString(currentpos, (String)val);
      else
         mypage.setInt(currentpos, (Integer)val);
      currentpos += size(val);
      */
	   
      //Edit       
       if (val instanceof String)
           logBuffer.contents.setString(currentpos, (String)val);
       
        else
           logBuffer.contents.setInt(currentpos, (Integer)val);
        currentpos += size(val);
   }

   /**
    * Calculates the size of the specified integer or string.
    * @param val the value
    * @return the size of the value, in bytes
    */
   private int size(Object val) {
      if (val instanceof String) {
         String sval = (String) val;
         return STR_SIZE(sval.length());
      }
      else
         return INT_SIZE;
   }

   /**
    * Returns the LSN of the most recent log record.
    * As implemented, the LSN is the block number where the record is stored.
    * Thus every log record in a block has the same LSN.
    * @return the LSN of the most recent log record
    */
   private int currentLSN() {
      return currentblk.number();
   }

   /**
    * Writes the current page to the log file.
    * 
    * Edit:
    * Change the write functionality by not writing into the page
    * and let the log_buffer be flushed
    *  @author pgupta (pgupta9)
    */
   private void flush() {
      //mypage.write(currentblk);
      //Edit
      logBuffer.flushLog();
   }

   /**
    * Clear the current page, and append it to the log file
    * 
    * Edit
    * Changed the appendNewblock startegy to pin a new Buffer for the blocks in the log file
    * using pinNew method to retrieve a new buffer from the buffer pool
    * 
    *  @author neetishpathak
    */
   private synchronized void appendNewBlock() {
      /*setLastRecordPosition(0);
      currentpos = INT_SIZE;
      currentblk = mypage.append(logfile);*/
      
      //Edit
      LogFormatter lfm = new LogFormatter();
      logBuffer = SimpleDB.bufferMgr().pinNew(logfile, lfm);
      currentblk = logBuffer.block(); 
      setLastRecordPosition(0);
      currentpos = INT_SIZE;
   }

   /**
    * Sets up a circular chain of pointers to the records in the page.
    * There is an integer added to the end of each log record
    * whose value is the offset of the previous log record.
    * The first four bytes of the page contain an integer whose value
    * is the offset of the integer for the last log record in the page.
    * 
    * Edit
    * Used logBuffer's setInt method to finalize a record 
    * 
    * @author Pratyush Gupta (pgupta9)
    */
   private void finalizeRecord() {
     /* mypage.setInt(currentpos, getLastRecordPosition());
      setLastRecordPosition(currentpos);
      currentpos += INT_SIZE;*/
      
      //Edit
      logBuffer.contents.setInt(currentpos, getLastRecordPosition());
      setLastRecordPosition(currentpos);
      currentpos += INT_SIZE;
      
   }

   /*Edit
    * getLastRecord now uses the getInt of the buffer class
    * 
    *  @author Mohit Satarkar
    * */
   private int getLastRecordPosition() {
      //return mypage.getInt(LAST_POS);
      //Edit
      return logBuffer.getInt(LAST_POS);
   }

   /*Edit
    * setLastRecordPosition now uses the setInt of the buffer class
    * 
    *  @author Neetish Pathak
    * */
   private void setLastRecordPosition(int pos) {
      //mypage.setInt(LAST_POS, pos);
      //Edit
      logBuffer.contents.setInt(LAST_POS, pos);
   }
}
