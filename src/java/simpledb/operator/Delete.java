package simpledb.operator;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.exception.DbException;
import simpledb.Type;
import simpledb.exception.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.field.IntField;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private final TupleDesc tupleDesc;
    private OpIterator[] children;
    private boolean hasNoMoreElements;

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
        this.children = new OpIterator[1];
        this.children[0] = child;

        Type[] types = new Type[1];
        String[] names = new String[1];
        types[0] = Type.INT_TYPE;
        names[0] = "deletedRecords";
        this.tupleDesc = new TupleDesc(types, names);
        this.hasNoMoreElements = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
        this.hasNoMoreElements = false;
    }

    public void close() {
        // some code goes here
        super.close();
        children[0].close();
        this.hasNoMoreElements = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
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
        if (hasNoMoreElements) {
            return null;
        }

        Tuple deletedResult = new Tuple(this.getTupleDesc());
        int records = 0;
        while(children[0].hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(this.transactionId, children[0].next());
                records++;
            } catch (IOException e) {
                // TODO log this exception message
                e.printStackTrace();
            }
        }
        deletedResult.setField(0, new IntField(records));
        this.hasNoMoreElements = true;
        return deletedResult;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.children = children;
    }

}
