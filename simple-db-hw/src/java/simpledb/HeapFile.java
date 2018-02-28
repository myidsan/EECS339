package simpledb;

import java.io.*;
import java.nio.Buffer;
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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private int id;
    private File file;
    private TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.id = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the Frile backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.id;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(this.file,"r");
            int offset = BufferPool.getPageSize() * pid.getPageNumber();
            byte[] data = new byte[BufferPool.getPageSize()];
            if (offset + BufferPool.getPageSize() > file.length()) {
                System.err.println("Page offset exceeds max size, error!");
                System.exit(1);
            }
            raf.seek(offset);
            raf.readFully(data);
            raf.close();
            return new HeapPage((HeapPageId) pid, data);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException(e.getMessage());
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
    	PageId pid = page.getId();
    	// int offset = BufferPool.PAGE_SIZE * pid.getPageNumber();
    	int offset = BufferPool.getPageSize() * pid.getPageNumber();
    	raf.seek(offset);
    	// raf.write(page.getPageData(), 0, BufferPool.PAGE_SIZE);
    	raf.write(page.getPageData(), 0, BufferPool.getPageSize());
    	raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        // return (int) Math.ceil(this.file.length()/BufferPool.PAGE_SIZE);
        return (int) Math.ceil(this.file.length()/BufferPool.getPageSize());
    }

    // Additional API Implemented
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
        
        RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
        // int offset = BufferPool.PAGE_SIZE * this.numPages();
        int offset = BufferPool.getPageSize()* this.numPages();
        raf.seek(offset);
        byte[] newHeapPageData = newHeapPage.getPageData();
        // raf.write(newHeapPageData, 0, BufferPool.PAGE_SIZE);
        raf.write(newHeapPageData, 0, BufferPool.getPageSize());
        raf.close();
        
        return new ArrayList<Page> (Arrays.asList(newHeapPage));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();
        HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        hpage.deleteTuple(t);
        return new ArrayList<Page> (Arrays.asList(hpage));
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this, tid);
    }

    /**
     * Helper class that implements the Java Iterator for tuples on a HeapFile
     */
    class HeapFileIterator extends AbstractDbFileIterator {

    	/**
    	 * An iterator to tuples for a particular page.
    	 */
        Iterator<Tuple> m_tupleIt;
       
        /**
         * The current number of the page this class is iterating through.
         */
        int m_currentgetPageNumber;

        /**
         * The transaction id for this iterator.
         */
        TransactionId m_tid;
        
        /**
         * The underlying heapFile.
         */
        HeapFile m_heapFile;

        /**
         * Set local variables for HeapFile and Transactionid
         * @param hf The underlying HeapFile.
         * @param tid The transaction ID.
         */
        public HeapFileIterator(HeapFile hf, TransactionId tid) {            
        	m_heapFile = hf;
            m_tid = tid;
        }

        /**
         * Open the iterator, must be called before readNext.
         */
        public void open() throws DbException, TransactionAbortedException {
            m_currentgetPageNumber = -1;
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            
        	// If the current tuple iterator has no more tuples.
        	if (m_tupleIt != null && !m_tupleIt.hasNext()) {	
                m_tupleIt = null;
            }

        	// Keep trying to open a tuple iterator until we find one of run out of pages.
            while (m_tupleIt == null && m_currentgetPageNumber < m_heapFile.numPages() - 1) {
                m_currentgetPageNumber++;		// Go to next page.
                
                // Get the iterator for the current page
                HeapPageId currentPageId = new HeapPageId(m_heapFile.getId(), m_currentgetPageNumber);
                                
                HeapPage currentPage = (HeapPage) Database.getBufferPool().getPage(m_tid,
                        currentPageId, Permissions.READ_ONLY);
                m_tupleIt = currentPage.iterator();
                
                // Make sure the iterator has tuples in it
                if (!m_tupleIt.hasNext())
                    m_tupleIt = null;
            }

            // Make sure we found a tuple iterator
            if (m_tupleIt == null)
                return null;
            
            // Return the next tuple.
            return m_tupleIt.next();
        }

        /**
         * Rewind closes the current iterator and then opens it again.
         */
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Close the iterator, which resets the counters so it can be opened again.
         */
        public void close() {
            super.close();
            m_tupleIt = null;
            m_currentgetPageNumber = Integer.MAX_VALUE;
        }
    }

}

