package simpledb.operator;

import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.util.NoSuchElementException;

/**
 * Abstract class for implementing operators. It handles <code>close</code>,
 * <code>next</code> and <code>hasNext</code>. Subclasses only need to implement
 * <code>open</code> and <code>readNext</code>.
 */
public abstract class Operator implements OpIterator {

    private static final long serialVersionUID = 1L;

    private Tuple next = null;
    private boolean open = false;
    private int estimatedCardinality = 0;

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!this.open) {
            throw new IllegalStateException("Operator not yet open");
        }
        
        if (next == null) {
            next = fetchNext();
        }
        return next != null;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = fetchNext();
            if (next == null) {
                throw new NoSuchElementException();
            }
        }

        Tuple result = next;
        next = null;
        return result;
    }



    /**
     * Closes this iterator. If overridden by a subclass, they should call
     * super.close() in order for Operator's internal state to be consistent.
     */
    @Override
    public void close() {
        // Ensures that a future call to next() will fail
        next = null;
        this.open = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.open = true;
    }

    /**
     * @return The estimated cardinality of this operator. Will only be used in
     *         lab7
     * */
    public int getEstimatedCardinality() {
        return this.estimatedCardinality;
    }

    /**
     * @param card
     *            The estimated cardinality of this operator Will only be used
     *            in lab7
     * */
    public void setEstimatedCardinality(int card) {
        this.estimatedCardinality = card;
    }



    /**
     * @return return the children DbIterators of this operator. If there is
     *         only one child, return an array of only one element. For join
     *         operators, the order of the children is not important. But they
     *         should be consistent among multiple calls.
     * */
    public abstract OpIterator[] getChildren();

    /**
     * Set the children(child) of this operator. If the operator has only one
     * child, children[0] should be used. If the operator is a join, children[0]
     * and children[1] should be used.
     *
     *
     * @param children
     *            the DbIterators which are to be set as the children(child) of
     *            this operator
     * */
    public abstract void setChildren(OpIterator[] children);

    /**
     * @return return the TupleDesc of the output tuples of this operator
     * */
    public abstract TupleDesc getTupleDesc();

    /**
     * Returns the next Tuple in the iterator, or null if the iteration is
     * finished. Operator uses this method to implement both <code>next</code>
     * and <code>hasNext</code>.
     *
     * @return the next Tuple in the iterator, or null if the iteration is
     *         finished.
     */
    protected abstract Tuple fetchNext() throws DbException,
            TransactionAbortedException;

}
