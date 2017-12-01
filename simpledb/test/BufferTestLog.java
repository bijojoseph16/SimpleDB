package simpledb.test;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import simpledb.remote.RemoteDriverImpl;
import simpledb.server.SimpleDB;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;
import simpledb.log.LogMgr;


public class BufferTestLog {

      private static Registry reg;
      private static final int DEFAULT_PORT = 1099;
      private static final String BINDING_NAME = "simpledb";
      
      private static void test1(Buffer lbuf, BufferMgr bmgr) {
          /*Test 1*/
          /*Write AB and CD in the logbuffer using the setString Function
           *Since logbMgr takes care of the offsets and txnum and lsn, we just make those values 0 here and pass the val appropriately
           * */
          lbuf.setString(0, "AB",0,0);
          lbuf.setString(0,"CD",0,0);
          
          printLogBuffer(bmgr);
      }

      private static void test2(Buffer lbuf, BufferMgr bmgr) {
          /*Test 2*/
          /*Write 0-9 in the logbuffer using the setInt Function
           *Since logbMgr takes care of the offsets and txnum and lsn, we just make those values 0 here and pass the val appropriately
           */
          int cnt = 1;
          while(cnt-- > 0) {
  	        for(int x=48; x <= 57; ++x) {
  	        		lbuf.setInt(0, x, 0, 0);
  	        }
          }
          
          printLogBuffer(bmgr);
      }

      private static void test3(Buffer lbuf, BufferMgr bmgr) {
    	  
          /*Test 3
           * Write a 100 * 10 of entries in i.e. (0-9) for hundred times, so that we can see that
           * proper new block allocation takes place 
           * */
    	  	  int cnt = 100;
          while(cnt-- > 0) {
  	        for(int x=48; x <= 57; ++x) {
  	        		lbuf.setInt(0, x, 0, 0);
  	        }
          }
    	  	  printLogBuffer(bmgr);
      }
      
      private static void test4(Buffer lbuf, BufferMgr bmgr) {
    	  
          /*Test 3
           * Write a 100 entries "This is log number" , so that we can see that
           * proper new block allocation takes place 
           * */
    	  	  int cnt = 1;
          while(cnt++ <= 100) {
  	        		String str = new String("This is log Num " + cnt);
  	        		lbuf.setString(0, str , 0, 0);
  	        
          }
    	  	  printLogBuffer(bmgr);
      }
      
      public static void main(String[] args) throws RemoteException, NotBoundException, InterruptedException {
        
        System.out.println("Starting");
        
        SimpleDB.init("simpleDB");
        reg = LocateRegistry.createRegistry(DEFAULT_PORT);
        reg.rebind(BINDING_NAME,new RemoteDriverImpl());
        System.out.println("Set Up Complete");
        
        new SimpleDB();
        BufferMgr buffMgr = SimpleDB.bufferMgr();
        
        SimpleDB.logMgr();
		/*Testing Logmanager*/
        Buffer lbuf = LogMgr.logBuffer;
        
      //Writing new values will be appended in the logFile
        
        test1(lbuf,buffMgr);
        //test2(lbuf, buffMgr);
        //test3(lbuf, buffMgr);
        //test4(lbuf, buffMgr);
        
        System.out.println("TearDown");
        reg.unbind(BINDING_NAME);
        reg = null;
        
        
        return;   
      }
      
      /*
       * To print the contents of the current log buffer
       * @author neetishpathak
       * */
      private static void printLogBuffer(BufferMgr bufferMgr) {
    	  	  int i=0;
    	  	  Block blk = SimpleDB.logMgr().logBuffer.block();
    	  	  Buffer buf = SimpleDB.logMgr().logBuffer;
    	  	  System.out.println("The values in the current buffer assigned to the block from the log file are: ");
    	  	  System.out.println("\t" + (++i) + ": " + blk.toString() + " = " + buf.toString() + "\t");
    	  	  
    	  	  System.out.println("--------------------------------------------------------------------------------------------");
    	  	  System.out.println("Please check the simpledb1.log file in your home_directory/simpleDB for byte buffer dumped. "
    	  	  		+ "Though it will be unreadable being in encoded format");
    	  	
      }
      
      /*
       * To print the contents of all the available buffers in the bufferPool buffer
       * @author bjoseph
       * */
      
      private static void printBufferPool(BufferMgr basicBufferMgr) {
          int i = 0;
          for (Map.Entry<Block, Buffer> e : basicBufferMgr.getBufferPoolMap().entrySet()) {
              System.out.println("\t" + (++i) + ": " + e.getKey().toString() + " = " + e.getValue().toString() + "\t");
          }
      }
}

