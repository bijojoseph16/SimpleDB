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


public class BufferTest4 {

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
        
        //Test Scenario 4
        //Test1 for BufferAbort Exception
        //Make change in SimpleDB.java, BUFFER_SIZE = 4;
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

        for (int i = 0; i < 3; i++) {
            Block block = blocks[i];
            System.out.println("\tPinning Block " + block);
            Buffer buffer = basicBufferMgr.pin(block);
            System.out.println("\tBlock Pinned to Buffer " + buffer);
            buffers[i] = buffer;
            
        }

        System.out.println("Buffer Pool after setting 3 blocks");
        printBufferPool(basicBufferMgr);

        try {
        System.out.println("Pinning new Block 3");
        System.out.println("Now we have no unpinned buffers available.");
        System.out.println("Buffer Abort Exception should be thrown.");
        
        basicBufferMgr.pin(blocks[3]);
        
        System.out.println("Buffer Abort Exception not thrown: Test Fail");
        
        } catch (BufferAbortException e) {
            System.out.println("Buffer Abort Exception thrown as no unpinned buffers available: Test Pass");
        }
        
        System.out.println("Buffer Pool after pinning new block 3:");
        printBufferPool(basicBufferMgr);

        basicBufferMgr.clearBufferPoolMap();
        
        //TODO: We also need to reset the numAvailable Flag here.
        System.out.println("----------Buffer Test Scenario 4 Run Complete----------");
        
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
