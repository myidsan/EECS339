package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate mem_pred;
    private OpIterator mem_opit;

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
        mem_pred = p;
        mem_opit = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return mem_pred;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return mem_opit.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        mem_opit.open();
    }

    public void close() {
        // some code goes here
        super.close();
        mem_opit.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        mem_opit.rewind();
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
        while (mem_opit.hasNext()) {
            Tuple tup = mem_opit.next();
            if (mem_pred.filter(tup)) {
                return tup;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new  OpIterator[] {mem_opit};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        mem_opit = children[0];
    }

}
