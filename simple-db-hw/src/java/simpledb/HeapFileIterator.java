package simpledb;

import java.util.*;

class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> iter = null;
    int cur_pgno = 0;

    TransactionId tid;
    HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
      this.hf = hf;
      this.tid = tid;
    }

    public void open() throws DbException, TransactionAbortedException {
      this.cur_pgno = -1;
    }

    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
      if (this.iter != null && !this.iter.hasNext())
        this.iter = null;

      while (this.iter == null && this.cur_pgno < this.hf.numPages() - 1) {
        this.cur_pgno++;
        HeapPageId curpid = new HeapPageId(this.hf.getId(), this.cur_pgno);
        HeapPage curp = (HeapPage) Database.getBufferPool().getPage(this.tid,
                curpid, Permissions.READ_ONLY);
        this.iter = curp.iterator();
        if (!this.iter.hasNext())
            this.iter = null;
      }

     if (this.iter == null)
       return null;
     return this.iter.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
      close();
      open();
    }

    public void close() {
      // to override we need to use super
      super.close();
      this.iter = null;
      this.cur_pgno = Integer.MAX_VALUE;
    }
}
