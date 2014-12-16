package simpledb.buffer;

import simpledb.file.*;
import simpledb.server.SimpleDB;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   private int bufferSize = -1;			//store the buffer size for my clock calculations
   private int clockHandPosition = -1;  // Increment first, check next. First increment makes this 0 which is a valid position
   private int maxRefCountValue = 5;
   private Map<Block, Buffer> bufferPoolMap = new HashMap<Block, Buffer>();
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
	  bufferSize = numbuffs;
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      maxRefCountValue = SimpleDB.getgClockRefCount();
      if(maxRefCountValue < 1)
    	  maxRefCountValue = 5;

      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer(maxRefCountValue);
         bufferpool[i].setMyIndex(i);
      }
   }
	
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
	/************************** HASH MAP IMPLEMENTATION *************************/
	synchronized void flushAll(int txnum) {
		Iterator<Map.Entry<Block, Buffer>> mapItr = bufferPoolMap.entrySet().iterator();
		Buffer buffTemp = null;
		Map.Entry<Block,Buffer> entry = null;
		while (mapItr.hasNext())
		{
			entry = (Map.Entry<Block,Buffer>)mapItr.next();
			buffTemp = (Buffer)entry.getValue();
			if (buffTemp.isModifiedBy(txnum)){
				buffTemp.flush();
				//mapItr.remove(); //Shud stay in hash map until replacement
			}
			
		}
   }
	
	/************************** DEFAULT IMPLEMENTATION *************************/
	/*synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.isModifiedBy(txnum))
				buff.flush();
	}*/
	
	/**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
         buff = chooseUnpinnedBuffer();
         
         if (buff == null)
            return null;
         buff.assignToBlock(blk);
         bufferPoolMap.put(blk, buff);    
      }
//      if (!buff.isPinned())
 //        numAvailable--;
      buff.pin();
      printParams();
      return buff;
   }
   
   /**
    Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
//      numAvailable--;
      buff.pin();
      bufferPoolMap.put(buff.block(), buff);
      
      printParams();
      
      return buff;
   }
   
   private void printParams() {
	// TODO Auto-generated method stub
	for(Buffer buff: bufferpool)
		buff.printBufferParams();
	System.out.println("Num avalable: " + available());
}


/**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      printParams();
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
	   int temp = 0;
	   for (Buffer buff : bufferpool)
		   if(!buff.isPinned())
			   temp++;
	   return temp; // (numAvailable - bufferPoolMap.size());
   }
   
   /****************************HASH MAP IMPLEMENTATION **************************************/
   private Buffer findExistingBuffer(Block blk) {
	   return (bufferPoolMap.get(blk));
   }
   
   /*************************** DEFAULT IMPLEMETATION ****************************************/
   /*private Buffer findExistingBuffer(Block blk) {
 
    for (Buffer buff : bufferpool) {
    	Block b = buff.block();
       if (b != null && b.equals(blk))
          return buff;
    }
    return null;
 }*/
   
   private Buffer chooseUnpinnedBuffer() { 
	  
	   //Return quickly incase of empty existing buffer in the bufferpool
		if (bufferPoolMap.size() < numAvailable) {
			for (Buffer buff : bufferpool) {
				if (!bufferPoolMap.containsValue(buff))
					return buff;
			}
		}

		// this is to prevent an infinite loop if all buffers were pinned
		int count = bufferSize * (maxRefCountValue + 1);

		while (count > 0) {
			
			//increment clockHandPosition but limit it to an index within bufferSize
			clockHandPosition = clockHandPosition + 1;
			clockHandPosition = clockHandPosition % bufferSize; // go from 0 to bufferSize - 1
			
			Buffer buff = bufferpool[clockHandPosition];
			
			//if the buffer is not pinned, then either return it or decrement its reference count value
			if (!buff.isPinned()) {
				//using "<1" because initial value is -1 and it could be 0 also.
				//The isPinned provides check in case of -1 and pinned condition
				if (buff.getRefCount() < 1) {
					//return buffer for replacement, remove from hash map
					System.out.println("G Clock: Buffer sent for replacement B" + buff.getMyIndex() 
							+ " Block details: " + buff.block().fileName() + ", " + buff.block().number());
					bufferPoolMap.remove(buff.block());
					return buff;
				}
				else {
					buff.decrementRefCount();
				}
			}
			
			count--;
		}
		
		//coudn't find an empty buffer so return null instead.
		return null;
   }
   
   /**  
   * Determines whether the map has a mapping from  
   * the block to some buffer.  
   * @param blk the block to use as a key  
   * @return true if there is a mapping; false otherwise  
   */  
   boolean containsMapping(Block blk) {  
   return bufferPoolMap.containsKey(blk);  
   } 

   /**  
   * Returns the buffer that the map maps the specified block to.  
   * @param blk the block to use as a key  
   * @return the buffer mapped to if there is a mapping; null otherwise  
   */  
   Buffer getMapping(Block blk) {  
   return bufferPoolMap.get(blk);  
   } 
}