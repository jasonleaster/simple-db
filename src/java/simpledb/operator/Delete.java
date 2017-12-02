package simpledb.operator;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.DbException;
import simpledb.Transaction;
import simpledb.exception.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private OpIterator opIterator;

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
        this.transactionId = t;
        this.opIterator = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return opIterator.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    public void close() {
        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
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
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
