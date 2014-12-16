package simpledb.buffer;

import simpledb.server.SimpleDB;
import simpledb.file.*;

/**
 * An individual buffer. A buffer wraps a page and stores information about its
 * status, such as the disk block associated with the page, the number of times
 * the block has been pinned, whether the contents of the page have been
 * modified, and if so, the id of the modifying transaction and the LSN of the
 * corresponding log record.
 * 
 * @author Edward Sciore
 */
public class Buffer {
	private Page contents = new Page();
	private Block blk = null;
	private int pins = 0;
	private int modifiedBy = -1; // negative means not modified
	private int logSequenceNumber = -1; // negative means no corresponding log
										// record
	private int refCount = -1;
	private int maxRefCountValue = -1;
	private int myIndex = -1;
	private int BLOCK_SIZE = 400;

	public int getMyIndex() {
		return myIndex;
	}

	public void setMyIndex(int myIndex) {
		this.myIndex = myIndex;
	}

	/**
	 * Creates a new buffer, wrapping a new {@link simpledb.file.Page page}.
	 * This constructor is called exclusively by the class
	 * {@link BasicBufferMgr}. It depends on the {@link simpledb.log.LogMgr
	 * LogMgr} object that it gets from the class
	 * {@link simpledb.server.SimpleDB}. That object is created during system
	 * initialization. Thus this constructor cannot be called until
	 * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or is called
	 * first.
	 */
	public Buffer() {
		maxRefCountValue = 5;
	}

	public Buffer(int i) {
		maxRefCountValue = i;
	}

	/**
	 * Returns the integer value at the specified offset of the buffer's page.
	 * If an integer was not stored at that location, the behavior of the method
	 * is unpredictable.
	 * 
	 * @param offset
	 *            the byte offset of the page
	 * @return the integer value at that offset
	 */
	public int getInt(int offset) {
		return contents.getInt(offset);
	}

	/**
	 * Returns the string value at the specified offset of the buffer's page. If
	 * a string was not stored at that location, the behavior of the method is
	 * unpredictable.
	 * 
	 * @param offset
	 *            the byte offset of the page
	 * @return the string value at that offset
	 */
	public String getString(int offset) {
		return contents.getString(offset);
	}

	/**
	 * Writes an integer to the specified offset of the buffer's page. This
	 * method assumes that the transaction has already written an appropriate
	 * log record. The buffer saves the id of the transaction and the LSN of the
	 * log record. A negative lsn value indicates that a log record was not
	 * necessary.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * @param val
	 *            the new integer value to be written
	 * @param txnum
	 *            the id of the transaction performing the modification
	 * @param lsn
	 *            the LSN of the corresponding log record
	 */
	public void setInt(int offset, int val, int txnum, int lsn) {
		
		// save the block before it is first modified
		if (modifiedBy == -1) {
			saveBlock();
		}

		modifiedBy = txnum;
		if (lsn >= 0)
			logSequenceNumber = lsn;
		contents.setInt(offset, val);
	}

	/**
	 * Writes a string to the specified offset of the buffer's page. This method
	 * assumes that the transaction has already written an appropriate log
	 * record. A negative lsn value indicates that a log record was not
	 * necessary. The buffer saves the id of the transaction and the LSN of the
	 * log record.
	 * 
	 * @param offset
	 *            the byte offset within the page
	 * @param val
	 *            the new string value to be written
	 * @param txnum
	 *            the id of the transaction performing the modification
	 * @param lsn
	 *            the LSN of the corresponding log record
	 */
	public void setString(int offset, String val, int txnum, int lsn) {
		
		//save the block before it is first modified
		if(modifiedBy == -1) {
			saveBlock();
		}
			
		modifiedBy = txnum;
		if (lsn >= 0)
			logSequenceNumber = lsn;
		contents.setString(offset, val);
	}

	/**
	 * Returns a reference to the disk block that the buffer is pinned to.
	 * 
	 * @return a reference to a disk block
	 */
	public Block block() {
		return blk;
	}

	/**
	 * Writes the page to its disk block if the page is dirty. The method
	 * ensures that the corresponding log record has been written to disk prior
	 * to writing the page to disk.
	 */
	void flush() {
		if (modifiedBy >= 0) {
			SimpleDB.logMgr().flush(logSequenceNumber);
			contents.write(blk);
			modifiedBy = -1;
		}
	}

	/**
	 * Increases the buffer's pin count.
	 */
	void pin() {
		// reset the reference count variable to -1 in case this was unpinned
		// and modiifed by clock
		if (pins == 0) {
			refCount = -1;
		}
		pins++;
	}

	/**
	 * Decreases the buffer's pin count.
	 */
	void unpin() {
		pins--;

		// Initialize the ref count for G Clock now as no one is using this
		// buffer anymore
		if (pins < 1) {
			refCount = maxRefCountValue;
		}
	}

	/**
	 * Returns true if the buffer is currently pinned (that is, if it has a
	 * nonzero pin count).
	 * 
	 * @return true if the buffer is pinned
	 */
	boolean isPinned() {
		return pins > 0;
	}

	/**
	 * Returns true if the buffer is dirty due to a modification by the
	 * specified transaction.
	 * 
	 * @param txnum
	 *            the id of the transaction
	 * @return true if the transaction modified the buffer
	 */
	boolean isModifiedBy(int txnum) {
		return txnum == modifiedBy;
	}

	/**
	 * Reads the contents of the specified block into the buffer's page. If the
	 * buffer was dirty, then the contents of the previous page are first
	 * written to disk.
	 * 
	 * @param b
	 *            a reference to the data block
	 */
	void assignToBlock(Block b) {
		flush();
		blk = b;
		contents.read(blk);
		pins = 0;
		refCount = -1;
	}

	/**
	 * Initializes the buffer's page according to the specified formatter, and
	 * appends the page to the specified file. If the buffer was dirty, then the
	 * contents of the previous page are first written to disk.
	 * 
	 * @param filename
	 *            the name of the file
	 * @param fmtr
	 *            a page formatter, used to initialize the page
	 */
	void assignToNew(String filename, PageFormatter fmtr) {
		flush();
		fmtr.format(contents);
		blk = contents.append(filename);
		pins = 0;
		refCount = -1;
	}

	/**
	 * Decrement the reference count value. Protected so that only unpinned
	 * buffers have reference count decremented. Pinned buffers have -1 by
	 * default.
	 */
	void decrementRefCount() {
		if (!isPinned()) {
			refCount--;
			if (refCount < -1)
				refCount = -1;
		}
	}

	/**
	 * return the reference count stored. -1 indicated buffer was pinned and
	 * should not be used.
	 * 
	 * @return refCount
	 */
	int getRefCount() {
		return refCount;
	}

	/**
	 * Save a block to disk. File and offset are specified in arguments.
	 * contents of buffer in page need to be written to disk.
	 */
	public void saveBlock() {
		Block save_block = new Block("bufferbackup", myIndex * BLOCK_SIZE);
		contents.write(save_block);
	} 
	
	public void restoreBlock(int txnum, int lsn, Block myBlock, int offset_file) {
		Block restore_block = new Block("bufferbackup", offset_file);
		contents.read(restore_block);
		
		this.blk = myBlock;
		this.modifiedBy = txnum;
		this.logSequenceNumber = lsn;
		
		flush();
	}

	public void printBufferParams() {
		// TODO Auto-generated method stub
		System.out.println("Buffer"+ myIndex+":  Pin Count: "+ pins + "  Ref Count: "+ refCount + " File Name: "+ ((blk != null )? blk.fileName():"- ") + " Blk Number: "+ ((blk != null)? blk.number():"-"));
		
	}
}