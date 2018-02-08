package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId t_tid;
    private OpIterator op_iter;
    private TupleDesc t_td;
    private boolean deleted;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        t_tid = t;
        op_iter = child;
        deleted = false;

        String[] names = new String[] {"Deleted"};
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
        deleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (deleted) 
            return null;

        int deletedCount = 0;

        while (op_iter.hasNext()) {
            Tuple tup = op_iter.next();
            try {
                Database.getBufferPool().deleteTuple(t_tid, tup);
            } 
            catch (IOException e) {
                throw new DbException("IO Exception on tuple delete");
            }
            deletedCount++;
        }
        Tuple resultTuple = new Tuple(t_td);
        resultTuple.setField(0, new IntField(deletedCount));
        deleted = true; 
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
