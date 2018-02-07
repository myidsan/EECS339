package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;
    private int tableid;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
        this.tableid = f.getAbsoluteFile().hashCode();

    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // refer to constructor
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try (RandomAccessFile raf = new RandomAccessFile(this.f, "r")) {
              long pos = pid.getPageNumber() * (long) BufferPool.getPageSize();
              if (pos < 0 || pos >= this.f.length()) {
                  throw new IllegalArgumentException("The page doesn't exist in this file.");
              }

              raf.seek(pos);
              byte[] buf = new byte[BufferPool.getPageSize()];
              raf.read(buf);
              return new HeapPage((HeapPageId) pid, buf);
          } catch (IOException e) {
              e.printStackTrace();
              System.exit(1);
          }
          return null;
  }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // adding API
    private HeapPage getFreePage(TransactionId tid) throws TransactionAbortedException, DbException
    {
    	for (int i = 0; i < this.numPages(); i++)
    	{
    		PageId pid = new HeapPageId(this.getId(), i);
    		HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        	if (hpage.getNumEmptySlots() > 0)
        		return hpage;
    	}
    	return null;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage hpage = getFreePage(tid);
        if (hpage != null)
        {
        	hpage.insertTuple(t);
        	return new ArrayList<Page> (Arrays.asList(hpage));
        }
        
        // no empty pages found, so create a new one
        HeapPageId newHeapPageId = new HeapPageId(this.getId(), this.numPages());
        HeapPage newHeapPage = new HeapPage(newHeapPageId, HeapPage.createEmptyPageData());
        newHeapPage.insertTuple(t);
        
        RandomAccessFile raf = new RandomAccessFile(this.f, "rw");
        int offset = BufferPool.getPageSize() * this.numPages();
        raf.seek(offset);
        byte[] newHeapPageData = newHeapPage.getPageData();
        raf.write(newHeapPageData, 0, BufferPool.getPageSize());
        raf.close();
        
        return new ArrayList<Page> (Arrays.asList(newHeapPage));
        
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }
}
