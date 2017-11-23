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


public class BufferTest {

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
        
        /*
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
        printBufferPool(basicBufferMgr);
        
        System.out.println("Now pin 3 Blocks");
        Buffer[] buffers = new Buffer[3];

        for (int i = 0; i < 3; i++) {
            Block block = blocks[i];
            System.out.println("\tPinning Block " + block);
            Buffer buffer = basicBufferMgr.pin(block);
            System.out.println("\tBlock Pinned to Buffer " + buffer);
            buffers[i] = buffer;
            TimeUnit.SECONDS.sleep(1);
        }

        System.out.println("Buffer Pool after setting 3 blocks");
        printBufferPool(basicBufferMgr);

        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 0");
        basicBufferMgr.unpin(buffers[0]);
        System.out.println("\tUnpining Block 0");
        basicBufferMgr.unpin(buffers[0]);

        System.out.println("\tUnpining Block 1");
        basicBufferMgr.unpin(buffers[1]);
        System.out.println("\tUnpining Block 1");
        basicBufferMgr.unpin(buffers[1]);

        System.out.println("Buffer Pool after unpinning blocks :");
        printBufferPool(basicBufferMgr);

        System.out.println("Pinning new Block 3");
        basicBufferMgr.pin(blocks[3]);
        System.out.println("Now we have 2 unpinned buffers available.");
        
        System.out.println("Buffer Pool after pinning new block 3:");
        printBufferPool(basicBufferMgr);

        if(!basicBufferMgr.getBufferPoolMap().containsKey(blocks[0])) {
            System.out.println("As per LRU2 block 0 has been removed");
        }
        if(basicBufferMgr.getBufferPoolMap().containsKey(blocks[3])) {
            System.out.println("Block 3 has been added");
        }
        basicBufferMgr.clearBufferPoolMap();
        System.out.println("Buffer pool after clearing");
        printBufferPool(basicBufferMgr);
        //TODO: We also need to reset the numAvailable Flag here.
        System.out.println("----------Buffer Test Scenario 1 Run Complete----------");
        */

        /*
        //Test2 algorithm should use LRU
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
        for (int i = 2; i >= 0; i--) {
            Block block = blocks[i];
            System.out.println("\tPinning Block " + block);
            Buffer buffer = basicBufferMgr.pin(block);
            System.out.println("\tBlock Pinned to Buffer " + buffer);
            buffers[i] = buffer;
            TimeUnit.SECONDS.sleep(1);
        }

        System.out.println("Buffer Pool after setting 3 blocks");
        printBufferPool(basicBufferMgr);

        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 1");
        basicBufferMgr.unpin(buffers[1]);
        System.out.println("\tUnpining Block 1");
        basicBufferMgr.unpin(buffers[1]);

        System.out.println("\tUnpining Block 0");
        basicBufferMgr.unpin(buffers[0]);
        System.out.println("\tUnpining Block 0");
        basicBufferMgr.unpin(buffers[0]);

        System.out.println("Buffer Pool after unpinning blocks :");
        printBufferPool(basicBufferMgr);

        System.out.println("Pinning new Block 3");
        basicBufferMgr.pin(blocks[3]);
        System.out.println("Now we have 2 unpinned buffers available and both these buffers have LRU2 infinity.");
        
        System.out.println("Buffer Pool after pinning new block 3:");
        printBufferPool(basicBufferMgr);

        if(!basicBufferMgr.getBufferPoolMap().containsKey(blocks[1])) {
            System.out.println("As per LRU block 1 has been removed");
        }
        if(basicBufferMgr.getBufferPoolMap().containsKey(blocks[3])) {
            System.out.println("Block 3 has been added");
        }
        basicBufferMgr.clearBufferPoolMap();
        System.out.println("Buffer pool after clearing");
        printBufferPool(basicBufferMgr);
        //TODO: We also need to reset the numAvailable Flag here.
        System.out.println("----------Buffer Test Scenario 1 Run Complete----------");
        */
        
        //Algorithm should default to LRU as there are 2 buffers 
        //with infinite LRU2 distance
        //Only use 1 to 10
        System.out.println("Create 11 Blocks");
        Block[] blocks = new Block[11];
                 
        for(int i = 1; i < 11; i++) {
            System.out.println("Creating Block " + (i));
            blocks[i] = new Block("filename", i);
        }

        System.out.println("Initial State of Buffer Pool:");
        printBufferPool(basicBufferMgr);
        
        System.out.println("Now pin 8 Blocks");
        Buffer[] buffers = new Buffer[9];
        for (int i = 1; i <= 8; i++) {
            Block block = blocks[i];
            System.out.println("\tPinning Block " + block);
            Buffer buffer = basicBufferMgr.pin(block);
            System.out.println("\tBlock Pinned to Buffer " + buffer);
            buffers[i] = buffer;
            //TimeUnit.SECONDS.sleep(1);
        }

        System.out.println("Now pin block 4");
        System.out.println("\tPinning Block " + blocks[4]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[4]));
        buffers[4] = basicBufferMgr.pin(blocks[4]);

        System.out.println("Now pin block 2");
        System.out.println("\tPinning Block " + blocks[2]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[2]));
        buffers[2] = basicBufferMgr.pin(blocks[2]);

        System.out.println("Now pin block 7");
        System.out.println("\tPinning Block " + blocks[7]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[7]));
        buffers[7] = basicBufferMgr.pin(blocks[7]);
        
        System.out.println("Now pin block 1");
        System.out.println("\tPinning Block " + blocks[1]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[1]));
        buffers[1] = basicBufferMgr.pin(blocks[1]);

        System.out.println("Buffer Pool after setting 8 blocks and pinning Blocks 4, 2, 7, 1:");
        printBufferPool(basicBufferMgr);

        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 8");
        basicBufferMgr.unpin(buffers[8]);
        
        System.out.println("\tUnpining Block 7");
        basicBufferMgr.unpin(buffers[7]);
        
        System.out.println("\tUnpining Block 6");
        basicBufferMgr.unpin(buffers[6]);
        
        System.out.println("\tUnpining Block 5");
        basicBufferMgr.unpin(buffers[5]);

        System.out.println("\tUnpining Block 4");
        basicBufferMgr.unpin(buffers[4]);

        System.out.println("\tUnpining Block 1");
        basicBufferMgr.unpin(buffers[1]);

        System.out.println("\tUnpining Block 7");
        basicBufferMgr.unpin(buffers[7]);
        
        System.out.println("\tUnpining Block 4");
        basicBufferMgr.unpin(buffers[4]);

        System.out.println("\tUnpining Block 2");
        basicBufferMgr.unpin(buffers[2]);

        System.out.println("\tUnpining Block 2");
        basicBufferMgr.unpin(buffers[2]);

        System.out.println("Buffer Pool after unpinning blocks :");
        printBufferPool(basicBufferMgr);

        System.out.println("Pinning new Block 8");
        basicBufferMgr.pin(blocks[9]);
        System.out.println("Now we have 6 unpinned buffers available.");
        System.out.println("There will be more than 1 buffer with LRU2 = infinity.");

        System.out.println("Buffer Pool after pinning new block 9:");
        printBufferPool(basicBufferMgr);

        if(!basicBufferMgr.getBufferPoolMap().containsKey(blocks[5])) {
            System.out.println("As per LRU block 5 has been removed");
        }
        if(basicBufferMgr.getBufferPoolMap().containsKey(blocks[9])) {
            System.out.println("As per block 9 has been ");
        }
        basicBufferMgr.getBufferPoolMap().clear();
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
