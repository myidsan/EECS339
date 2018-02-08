package simpledb;

import java.io.IOException;


/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId t_tid;
    private OpIterator op_iter;
    private int t_tableId;
    private boolean inserted;
    private TupleDesc t_td;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        t_tid = t;
        op_iter = child;
        t_tableId = tableId;
        inserted = false;

        String[] names = new String[] {"Inserted"}; 
        Type[] types = new Type[] {Type.INT_TYPE};
        t_td = new TupleDesc(types, names);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return t_td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        op_iter.open(); 
        inserted = false;
    }

    public void close() {
        // some code goes here
        super.close();
        op_iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        op_iter.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int insertedCount = 0;

        if (inserted) 
            return null;
    	
    	while (op_iter.hasNext())
    	{
    		Tuple tup = op_iter.next();
    		try 
    		{
        		Database.getBufferPool().insertTuple(t_tid, t_tableId, tup);		
    		}
    		catch (IOException e)
    		{
    			throw new DbException("IO Exception on tuple insertion");
    		}
    		insertedCount++;
    	}
    	Tuple resultTuple = new Tuple(t_td);
    	resultTuple.setField(0, new IntField(insertedCount));
    	inserted = true;
    	return resultTuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {op_iter};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        op_iter = children[0];
    }
}
