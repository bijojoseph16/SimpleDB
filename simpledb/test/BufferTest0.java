package simpledb.test;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import simpledb.remote.RemoteDriverImpl;
import simpledb.server.SimpleDB;

import simpledb.buffer.Buffer;
import simpledb.buffer.BufferAbortException;
import simpledb.buffer.BufferMgr;
import simpledb.file.Block;


public class BufferTest0 {

      private static Registry reg;
      private static final int DEFAULT_PORT = 1099;
      private static final String BINDING_NAME = "simpledb";
      
      public static void main(String[] args) throws RemoteException, NotBoundException, InterruptedException {
        
        System.out.println("Starting");
        SimpleDB.init("simpleDB");
        reg = LocateRegistry.createRegistry(DEFAULT_PORT);
        reg.rebind(BINDING_NAME,new RemoteDriverImpl());
        System.out.println("Set Up Complete");
        
        System.out.println("Setting up buffer");
        new SimpleDB();
        BufferMgr basicBufferMgr = SimpleDB.bufferMgr();
        
        //Test Secanrio 0
       //Make change in SimpleDB.java, BUFFER_SIZE = 3;
        System.out.println("Create 4 Blocks");
        Block[] blocks = new Block[4];
        
        //Only use 0 to 3
        for(int i = 0; i < 4 ; i++) {
            System.out.println("Creating Block " + (i));
            blocks[i] = new Block("filename", i);
        }

        System.out.println("Initial State of Buffer Pool:");
        printBufferPool(basicBufferMgr);
        
        
        System.out.println("Now pin 3 Blocks");
        Buffer[] buffers = new Buffer[3];
        
        System.out.println("----------Start Test pin ----------");
        for (int i = 0; i < 3; i++) {
            Block block = blocks[i];
            System.out.println("\tPinning Block " + block);
            Buffer buffer = basicBufferMgr.pin(block);
            System.out.println("\tBlock Pinned to Buffer " + buffer);
            buffers[i] = buffer;
        }

        System.out.println("Buffer Pool after setting 3 blocks");
        printBufferPool(basicBufferMgr);
        System.out.println("Test: Pin count of all the buffer should be 1");
        //Now all the buffers in the buffer pool should have pin count 1
        for(int i = 0; i < 3; i++) {
            Buffer buf = basicBufferMgr.getMapping(blocks[i]);
            //Now check pin count of each buffer it should be 1
            if(buf.getPinCount() == 1) {
          
            }
            else if (buf.getPinCount() != 1){
                System.out.println("All buffers don't have pin count 1: Test Fail");
                System.out.println("Block " + i + "pinned to"+buf+"does not have correct pin count");
            }
            if(i == 2) {
                System.out.println("All buffers  have pin count 1: Test Pass");
            }
              
        }
        
        //Now pin block 0 again
        System.out.println("Test: Pin Block 0, Pin count of the buffer block 0 is pinned to should be 2");
        basicBufferMgr.pin(blocks[0]);
        if(basicBufferMgr.getMapping(blocks[0]).getPinCount() == 2) {
            System.out.println("The pin count is 2: Test Pass");
        }
        else {
            System.out.println("The pin count is not 2: Test Fail");
        }
        System.out.println("----------End Test pin ----------\n");
        
        
        System.out.println("----------Start Test unpin ----------");
        
        System.out.println("Unpin block zero twice");
        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 0");
        basicBufferMgr.unpin(basicBufferMgr.getMapping(blocks[0]));
        System.out.println("\tUnpining Block 0");
        basicBufferMgr.unpin(basicBufferMgr.getMapping(blocks[0]));
        System.out.println("Test: The pin count of buffer associated with block 0 should be 0");
        
        if(basicBufferMgr.getMapping(blocks[0]).getPinCount() == 0) {
            System.out.println("The pin count is 0: Test Pass");
        }
        else {
            System.out.println("The pin count is not 0: Test Fail");
        }
        
        System.out.println("----------End Test unpin ----------\n");
        
        System.out.println("----------Start Test number of available Buffers ----------");
        
        System.out.println("Test: Now there should be 1 buffer availble as buffer assocaited with block 0 has pin count 0");
        if(basicBufferMgr.available() == 1) {
            System.out.println("There is 1 buffer availble: Test Pass");
        }
        else {
            if(basicBufferMgr.available() < 1)
              System.out.println("There  are no  buffers availble: Test Fail");
            else 
              System.out.println("There  are more  buffers availble: Test Fail");
        }
        System.out.println("----------End Test number of available Buffers ----------\n");
        
        System.out.println("----------Start Test buffer reallocation ----------");
        System.out.println("Current State of Buffer pool");
        printBufferPool(basicBufferMgr);
        System.out.println("\nTest: Relloaction should take place as the bufferPool is filled no empty buffers available:\n");
        System.out.println("\tPinning Block " + blocks[3]);
        basicBufferMgr.pin(blocks[3]);
        if(basicBufferMgr.getBufferPoolMap().containsKey(blocks[3]) && !basicBufferMgr.getBufferPoolMap().containsKey(blocks[0])) {
            printBufferPool(basicBufferMgr);
            System.out.println("Block 3 has been added by removing Block 0: Test Pass");
        }
        else {
            System.out.println("Block 3 has not been added: Test Fail");
        }
        
        System.out.println("----------End Test buffer reallocation ----------\n");
        
        System.out.println("----------Start Test buffer Abort Exception ----------");      
        
        System.out.println("Test: Try to pin block 0 Buffer Abort exception should be thrown");
        try {
        System.out.println("Pinning Block 0");
        System.out.println("Now we have no unpinned buffers available.");
        printBufferPool(basicBufferMgr);
        System.out.println("Buffer Abort Exception should be thrown.");
        
        basicBufferMgr.pin(blocks[0]);
        
        System.out.println("Buffer Abort Exception not thrown: Test Fail");
        
        } catch (BufferAbortException e) {
            System.out.println("Buffer Abort Exception thrown as no unpinned buffers available: Test Pass");
        }
        
        System.out.println("\nTest: Unpin block 1 and pin block 0 BufferAbort Should not be thrown");
        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 1");
        basicBufferMgr.unpin(basicBufferMgr.getMapping(blocks[1]));
        
        try {
        System.out.println("Pinning Block 0");
        System.out.println("Now we have  unpinned buffers available.");
        printBufferPool(basicBufferMgr);
        System.out.println("Buffer Abort Exception should not be thrown.");
        
        basicBufferMgr.pin(blocks[0]);
        
        System.out.println("Buffer Abort Exception not thrown: Test Pass");
        
        } catch (BufferAbortException e) {
            System.out.println("Buffer Abort Exception thrown : Test Fail");
        }
        

        
        System.out.println("----------End Test buffer Abort Exception ----------\n");
        

        basicBufferMgr.clearBufferPoolMap();
        System.out.println("Buffer pool after clearing");
        printBufferPool(basicBufferMgr);
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
              System.out.println("\t" + (++i) + ": " + e.getKey().toString()+", Pin count:" +"["+e.getValue().getPinCount() +"]" + " = [" + e.getValue().toString() + "]\t");
          }
      }
      
}
