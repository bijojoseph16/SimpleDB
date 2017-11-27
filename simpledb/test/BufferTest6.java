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


public class BufferTest6 {

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
        
        //Test Secanrio 6
        //Test bufferPoolMap 
        //If a mapping exits between block and buffer show the it
        //Else print no mapping exits
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
            //TimeUnit.SECONDS.sleep(1);
            System.out.println("\tBlock Pinned to Buffer " + buffer);
            buffers[i] = buffer;
        }

        System.out.println("Buffer Pool after setting 3 blocks");
        printBufferPool(basicBufferMgr);
         
        System.out.println("BufferPool should contain block 1, checking ..");
        if(basicBufferMgr.containsMapping(blocks[1])) {
            System.out.println("BufferPool contains block 1: Test Pass");
            System.out.println("\t" + ": " + blocks[1].toString() + " = [" + basicBufferMgr.getMapping(blocks[1]).toString() + "]\t");
        }
        else {
            System.out.println("BufferPool does not contain block 1: Test Fail");
        }

        System.out.println("BufferPool should not contain block 3, checking ..");
        if(!basicBufferMgr.containsMapping(blocks[3])) {
            System.out.println("BufferPool does not contain block 3: Test Pass");
        }
        else {
            System.out.println("BufferPool contains block 3: Test Fail");
        }
        
        basicBufferMgr.clearBufferPoolMap();
        System.out.println("Buffer pool after clearing");
        printBufferPool(basicBufferMgr);
        
        //TODO: We also need to reset the numAvailable Flag here.
        System.out.println("----------Buffer Test Scenario 6 Run Complete----------");

        
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
