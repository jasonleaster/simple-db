package simpledb.operator;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.Type;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.field.IntField;
import simpledb.transaction.TransactionId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId transactionId;
    private final int tableId;
    private final TupleDesc tupleDesc;
    private OpIterator[] children;
    private boolean hasNoMoreElements;

    /**
     * Constructor.
     *
     * @param tid
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId tid, OpIterator child, int tableId) throws DbException {
        this.transactionId = tid;
        this.children = new OpIterator[1];
        this.children[0] = child;
        this.tableId = tableId;

        Type[] types = new Type[1];
        String[] names = new String[1];
        types[0] = Type.INT_TYPE;
        names[0] = "returnRecords";
        this.tupleDesc = new TupleDesc(types, names);
        this.hasNoMoreElements = false;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
        this.hasNoMoreElements = false;
    }

    @Override
    public void close() {
        // some code goes here
        super.close();
        children[0].close();
        this.hasNoMoreElements = true;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
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
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (hasNoMoreElements) {
            return null;
        }

        Tuple insertResult = new Tuple(this.getTupleDesc());
        int records = 0;
        while(children[0].hasNext()) {
            try {
                Database.getBufferPool().insertTuple(transactionId, this.tableId, children[0].next());
                records++;
            } catch (IOException e) {
                // TODO log this exception message
                e.printStackTrace();
            }
        }
        insertResult.setField(0, new IntField(records));
        this.hasNoMoreElements = true;
        return insertResult;
    }

    @Override
    public OpIterator[] getChildren() {
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }
}
