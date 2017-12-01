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


public class BufferTest3 {

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
        
        //Test Secanrio 3
        //This is the test scenario given in the appendix
        //Algorithm should default to LRU as there are 2 buffers 
        //with infinite LRU2 distance
        //Only use 1 to 10
       //Make change in SimpleDB.java, BUFFER_SIZE = 9;
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

        System.out.println("Pinning new Block 9");
        basicBufferMgr.pin(blocks[9]);

        System.out.println("Buffer Pool after pinning new block 9:");
        printBufferPool(basicBufferMgr);

        System.out.println("Now we have 6 unpinned buffers available.(2, 4, 5, 6, 7, 8)");
        System.out.println("There will be more than 1 buffer with LRU2 = infinity.Buffers 5, 6, 8.Choose from 5, 6, 8.");
        System.out.println("As per LRU block 5 should be removed");
        if(!basicBufferMgr.getBufferPoolMap().containsKey(blocks[5])) {
            System.out.println("As per LRU block 5 has been removed: Test Pass");
        }
        else {
            System.out.println("Block 5 has not been removed: Test Fail");
        }
        
        System.out.println("Block 9 should be added ");
        if(basicBufferMgr.getBufferPoolMap().containsKey(blocks[9])) {
            System.out.println("Block 9 has been added: Test Pass");
        }
        else {
            System.out.println("Block 9 ws not added: Test Fail");
        }
        
        basicBufferMgr.getBufferPoolMap().clear();
        //TODO: We also need to reset the numAvailable Flag here.
        System.out.println("----------Buffer Test Scenario 3 Run Complete----------");
        
        
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
