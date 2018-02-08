package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate pred;
    private OpIterator op_iter;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        pred = p;
        op_iter = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return op_iter.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        op_iter.open();
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
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while (op_iter.hasNext()) {
            Tuple tup = op_iter.next();
            if (pred.filter(tup)) {
                return tup;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new  OpIterator[] {op_iter};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        op_iter = children[0];
    }

}
