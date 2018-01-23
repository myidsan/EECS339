package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private boolean isOpen = false;
    private TransactionId tid;
    private TupleDesc td;
    private DbFileIterator iter;
    private String tablename;
    private String alias;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        reset(tableid,tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return this.tablename;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return this.alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.isOpen=false;
        this.alias = tableAlias;
        // this.tablename = tableid;
        this.tablename = Database.getCatalog().getTableName(tableid);
        this.iter = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        td = Database.getCatalog().getTupleDesc(tableid);
        String[] newNames = new String[td.numFields()];
        Type[] newTypes = new Type[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            String name = td.getFieldName(i);
            Type t = td.getFieldType(i);

            newNames[i] = tableAlias + "." + name;
            newTypes[i] = t;
        }
        td = new TupleDesc(newTypes, newNames);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        if (isOpen)
            throw new DbException("double open on one DbIterator.");

        iter.open();
        isOpen = true;
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");
        return iter.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (!isOpen)
            throw new IllegalStateException("iterator is closed");

        return iter.next();

    }

    public void close() {
        iter.close();
        isOpen = false;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        close();
        open();
    }
}
