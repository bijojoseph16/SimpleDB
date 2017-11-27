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


public class BufferTestLog {

      private static Registry reg;
      private static final int DEFAULT_PORT = 1099;
      private static final String BINDING_NAME = "simpledb";
      
      public static void main(String[] args) throws RemoteException, NotBoundException, InterruptedException {
        
        System.out.println("Starting");
        SimpleDB.init("simpleDB1");
        reg = LocateRegistry.createRegistry(DEFAULT_PORT);
        reg.rebind(BINDING_NAME,new RemoteDriverImpl());
        System.out.println("Set Up Complete");
        
        System.out.println("Setting up buffer");
        new SimpleDB();
        BufferMgr buffMgr = SimpleDB.bufferMgr();
        
        /*Testing Logmanager*/
        int lsn1 = SimpleDB.logMgr().append(new Object[] {"a","b"});
        int lsn2 = SimpleDB.logMgr().append(new Object[] {"c","d"});
        int lsn3 = SimpleDB.logMgr().append(new Object[] {"e","f"});
        SimpleDB.logMgr().flush(lsn3);
        
        Iterator<BasicLogRecord> iter = SimpleDB.logMgr().iterator();
        int x = 0;
        while(iter.hasNext() && x++ < 3) {
        		BasicLogRecord rec = iter.next();
        		String v1 = rec.nextString();
        		String v2 = rec.nextString();
        		System.out.println("[" + v1 + ", " + v2 + "]" );
        }
        
        SimpleDB.logMgr().printLogPageBuffer();
        
        //Test Secanrio 1
        //Test1 for LRU2
        //Make change in SimpleDB.java, BUFFER_SIZE = 3;
        System.out.println("Create 4 Blocks");
        Block[] blocks = new Block[4];
        
        //Only use 0 to 3
        for(int i = 0; i < 4 ; i++) {
            System.out.println("Creating Block " + (i));
            blocks[i] = new Block("filename", i);
        }

        System.out.println("Initial State of Buffer Pool:");
        printBufferPool(buffMgr);
        
        System.out.println("Now pin 3 Blocks");
        Buffer[] buffers = new Buffer[3];

        for (int i = 0; i < 3; i++) {
            Block block = blocks[i];
            System.out.println("\tPinning Block " + block);
            Buffer buffer = buffMgr.pin(block);
            System.out.println("\tBlock Pinned to Buffer " + buffer);
            buffers[i] = buffer;
        }

        System.out.println("Buffer Pool after setting 3 blocks");
        printBufferPool(buffMgr);

        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 0");
        //buffers[0].setInt(0, 3, 100, );
        buffMgr.unpin(buffers[0]);
        System.out.println("\tUnpining Block 0");
        buffMgr.unpin(buffers[0]);

        System.out.println("\tUnpining Block 1");
        buffMgr.unpin(buffers[1]);
        System.out.println("\tUnpining Block 1");
        buffMgr.unpin(buffers[1]);

        System.out.println("Buffer Pool after unpinning blocks :");
        printBufferPool(buffMgr);

        System.out.println("Pinning new Block 3");
        buffMgr.pin(blocks[3]);        
        System.out.println("Buffer Pool after pinning new block 3:");
        printBufferPool(buffMgr);

        System.out.println("Now we have 2 unpinned buffers available, both have LRU2 infinity.");
        System.out.println("As per LRU2 block 0 should be removed");
        if(!buffMgr.getBufferPoolMap().containsKey(blocks[0])) {
            System.out.println("As per LRU2 block 0 has been removed: Test Pass");
        }
        else {
            System.out.println("As per LRU2 block 0 has not been removed: Test Fail");
        }
        System.out.println("Block 3 should have been added");
        if(buffMgr.getBufferPoolMap().containsKey(blocks[3])) {
            System.out.println("Block 3 has been added: Test Pass");
        }
        else {
            System.out.println("Block 3 has not been added: Test Fail");
        }
        buffMgr.clearBufferPoolMap();
        System.out.println("Buffer pool after clearing");
        printBufferPool(buffMgr);
        //TODO: We also need to reset the numAvailable Flag here.
        System.out.println("----------Buffer Test Scenario 1 Run Complete----------");

		
        
        System.out.println("TearDown");
        reg.unbind(BINDING_NAME);
        reg = null;
        
        
        return;   
      }
      
      private static void printBufferPool(BufferMgr basicBufferMgr) {
          int i = 0;
          for (Map.Entry<Block, Buffer> e : basicBufferMgr.getBufferPoolMap().entrySet()) {
              System.out.println("\t" + (++i) + ": " + e.getKey().toString() + " = [" + e.getValue().toString() + "]\t");
          }
      }
}

