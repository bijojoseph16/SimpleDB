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


public class BufferTest5 {

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
        
        //Test Secanrio 5
        //Test2 for LRU2 first LRU2 is used to remove
        //a buffer then the newly added block is unpinned
        //so as per LRU2, this block will have infinite
        //LRU2 distance, and is removed.
        //Make change in SimpleDB.java, BUFFER_SIZE = 9;
        System.out.println("Create 10 Blocks");
        Block[] blocks = new Block[10];
        
        //Create 10 blocks 0-9
        for(int i = 0; i < 10 ; i++) {
            System.out.println("Creating Block " + (i));
            blocks[i] = new Block("filename", i);
        }

        System.out.println("Initial State of Buffer Pool:");
        printBufferPool(basicBufferMgr);
        
        System.out.println("Now pin Blocks in the sequence 0,1,3,4,5,6,7,8,4,5,6,0,8,1,3,7");
        Buffer[] buffers = new Buffer[8];

        System.out.println("Now pin block 0");
        System.out.println("\tPinning Block " + blocks[0]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[0]));
        buffers[0] = basicBufferMgr.pin(blocks[0]);

        System.out.println("Now pin block 1");
        System.out.println("\tPinning Block " + blocks[1]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[1]));
        buffers[1] = basicBufferMgr.pin(blocks[1]);

        System.out.println("Now pin block 3");
        System.out.println("\tPinning Block " + blocks[3]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[3]));
        buffers[3] = basicBufferMgr.pin(blocks[3]);

        System.out.println("Now pin block 4");
        System.out.println("\tPinning Block " + blocks[4]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[4]));
        buffers[4] = basicBufferMgr.pin(blocks[4]);

        System.out.println("Now pin block 5");
        System.out.println("\tPinning Block " + blocks[5]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[5]));
        buffers[5] = basicBufferMgr.pin(blocks[5]);

        System.out.println("Now pin block 6");
        System.out.println("\tPinning Block " + blocks[6]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[6]));
        buffers[6] = basicBufferMgr.pin(blocks[6]);

        System.out.println("Now pin block 7");
        System.out.println("\tPinning Block " + blocks[7]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[7]));
        buffers[7] = basicBufferMgr.pin(blocks[7]);

        System.out.println("Now pin block 8");
        System.out.println("\tPinning Block " + blocks[8]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[8]));
        buffers[2] = basicBufferMgr.pin(blocks[8]);

        System.out.println("Now pin block 4");
        System.out.println("\tPinning Block " + blocks[4]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[4]));
        buffers[4] = basicBufferMgr.pin(blocks[4]);

        System.out.println("Now pin block 5");
        System.out.println("\tPinning Block " + blocks[5]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[5]));
        buffers[5] = basicBufferMgr.pin(blocks[5]);

        System.out.println("Now pin block 6");
        System.out.println("\tPinning Block " + blocks[6]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[6]));
        buffers[6] = basicBufferMgr.pin(blocks[6]);

        System.out.println("Now pin block 0");
        System.out.println("\tPinning Block " + blocks[0]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[0]));
        buffers[0] = basicBufferMgr.pin(blocks[0]);

        System.out.println("Now pin block 8");
        System.out.println("\tPinning Block " + blocks[8]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[8]));
        buffers[2] = basicBufferMgr.pin(blocks[8]);

        System.out.println("Now pin block 1");
        System.out.println("\tPinning Block " + blocks[1]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[1]));
        buffers[1] = basicBufferMgr.pin(blocks[1]);

        System.out.println("Now pin block 3");
        System.out.println("\tPinning Block " + blocks[3]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[3]));
        buffers[3] = basicBufferMgr.pin(blocks[3]);

        System.out.println("Now pin block 7");
        System.out.println("\tPinning Block " + blocks[7]);
        System.out.println("\tBlock Pinned to Buffer " + basicBufferMgr.pin(blocks[7]));
        buffers[7] = basicBufferMgr.pin(blocks[7]);

        printBufferPool(basicBufferMgr);

        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 8");
        basicBufferMgr.unpin(buffers[2]);
        System.out.println("\tUnpining Block 8");
        basicBufferMgr.unpin(buffers[2]);

        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 8");
        basicBufferMgr.unpin(buffers[2]);
        System.out.println("\tUnpining Block 8");
        basicBufferMgr.unpin(buffers[2]);

        
        System.out.println("\tUnpining Block 6");
        basicBufferMgr.unpin(buffers[6]);
        System.out.println("\tUnpining Block 6");
        basicBufferMgr.unpin(buffers[6]);

        System.out.println("\tUnpining Block 6");
        basicBufferMgr.unpin(buffers[6]);
        System.out.println("\tUnpining Block 6");
        basicBufferMgr.unpin(buffers[6]);

        System.out.println("\tUnpining Block 4");
        basicBufferMgr.unpin(buffers[4]);
        System.out.println("\tUnpining Block 4");
        basicBufferMgr.unpin(buffers[4]);

        System.out.println("\tUnpining Block 4");
        basicBufferMgr.unpin(buffers[4]);
        System.out.println("\tUnpining Block 4");
        basicBufferMgr.unpin(buffers[4]);

        System.out.println("\tUnpining Block 5");
        basicBufferMgr.unpin(buffers[5]);
        System.out.println("\tUnpining Block 5");
        basicBufferMgr.unpin(buffers[5]);

        System.out.println("\tUnpining Block 5");
        basicBufferMgr.unpin(buffers[5]);
        System.out.println("\tUnpining Block 5");
        basicBufferMgr.unpin(buffers[5]);

        System.out.println("Buffer Pool after unpinning blocks :");
        printBufferPool(basicBufferMgr);

        System.out.println("Pinning new Block 9");
        basicBufferMgr.pin(blocks[9]);
        System.out.println("Now we have 4 unpinned buffers available 2, 6, 4, and 5.");
        
        System.out.println("Buffer Pool after pinning new block 9:");
        printBufferPool(basicBufferMgr);

        System.out.println("As per LRU2 block 4 should be removed");
        if(!basicBufferMgr.getBufferPoolMap().containsKey(blocks[4])) {
            System.out.println("As per LRU2 block 4 has been removed: Test Pass");
        }
        else {
            System.out.println("As per LRU2 block 4 has not been removed: Test Fail");
        }

        System.out.println("Block 9 should be added");
        if(basicBufferMgr.getBufferPoolMap().containsKey(blocks[9])) {
            System.out.println("Block 9 has been added: Test Pass");
        }
        else {
            System.out.println("Block 9 has not been added: Test Fail");
        }
        
        System.out.println("Unpining Blocks");
        System.out.println("\tUnpining Block 9");
        basicBufferMgr.unpin(buffers[4]);
        System.out.println("Buffer Pool after unpinning block 9 :");
        printBufferPool(basicBufferMgr);

        System.out.println("Pinning new Block 2");
        basicBufferMgr.pin(blocks[2]);
        System.out.println("Buffer Pool after pinning block 2:");
        printBufferPool(basicBufferMgr);

        System.out.println("Buffer with infinite LRU2 distace, buffer where block 9 is stored should be removed"); 
        if(!basicBufferMgr.getBufferPoolMap().containsKey(blocks[9])) {
            System.out.println("Block 9 has been removed: Test Pass");
        }
        else {
            System.out.println("Did not switch to LRU block 9 has not been removed: Test Fail");
        }
        
        System.out.println("Block 2 should be added");
        if(basicBufferMgr.getBufferPoolMap().containsKey(blocks[2])) {
            System.out.println("Block 2 has been added: Test Pass");
        }
        else {
            System.out.println("Block 2 has not been added: Test Fail");
        }

        basicBufferMgr.clearBufferPoolMap();
        System.out.println("Buffer pool after clearing");
        printBufferPool(basicBufferMgr);
        //TODO: We also need to reset the numAvailable Flag here.
        System.out.println("----------Buffer Test Scenario 5 Run Complete----------");
        

        
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
