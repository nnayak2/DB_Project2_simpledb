package simpledb.tx.recovery;

import simpledb.server.SimpleDB;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;

class UpdateRecord implements LogRecord {
   private int txnum, offset_blk, offset_bckpFile;
   private String filename;
   private int BLOCK_SIZE = 400;
   
   /**
    * Creates a new UpdateRecord log record.
    * @param txnum the ID of the specified transaction
    * @param blk the block containing the value
    * @param offset the offset of the value in the block
    * @param val the new value
    */
   public UpdateRecord(int txnum, Buffer buff) {
      this.txnum = txnum;
      this.filename = buff.block().fileName();
      this.offset_blk = buff.block().number();
      this.offset_bckpFile = buff.getMyIndex() * BLOCK_SIZE;
   }
   
   /**
    * Creates a log record by reading five other values from the log.
    * @param rec the basic log record
    */
   public UpdateRecord(BasicLogRecord rec) {
	   
      txnum = rec.nextInt();
      filename = rec.nextString();
      offset_blk = rec.nextInt();
      offset_bckpFile = rec.nextInt();
  }
   
   /** 
    * Writes a Update record to the log.
    * This log record contains the SETSTRING operator,
    * followed by the transaction id, the filename, number,
    * and offset of the modified block, and the previous
    * string value at that offset.
    * @return the LSN of the last log value
    */
   public int writeToLog() {
	  Object[] rec = new Object[] {UPDATE, txnum, filename, offset_blk, offset_bckpFile};
      return logMgr.append(rec);
   }
   
   public int op() {
      return UPDATE;
   }
   
   public int txNumber() {
      return txnum;
   }
   
   public String toString() {
      return "<UPDATE " + txnum + " " + filename + " " + offset_blk + " " + offset_bckpFile +  ">";
   }
   
   /**
    * Replaces the specified data value with the value saved in the log record.
    * The method pins a buffer to the specified block,
    * calls  Update to restore the saved value
    * (using a dummy LSN), and unpins the buffer.
    * @see simpledb.tx.recovery.LogRecord#undo(int)
    */
   public void undo(int txnum) {
	      BufferMgr buffMgr = SimpleDB.bufferMgr();
	      Block blk = new Block(filename, offset_blk);
	      Buffer buff = buffMgr.pin(blk);
	      buff.restoreBlock(txnum, -1, blk, offset_bckpFile);
	      buffMgr.unpin(buff);
   }
}