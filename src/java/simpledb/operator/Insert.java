package simpledb.operator;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.DbException;
import simpledb.TransactionId;
import simpledb.Type;
import simpledb.exception.TransactionAbortedException;
import simpledb.field.IntField;
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
        this.transactionId = t;
        this.children = new OpIterator[1];
        this.children[0] = child;
        this.tableId = tableId;

        Type[] types = new Type[1];
        String[] names = new String[1];
        types[0] = Type.INT_TYPE;
        names[0] = "returnRecords";
        this.tupleDesc = new TupleDesc(types, names);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        children[0].open();
    }

    public void close() {
        // some code goes here
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!children[0].hasNext()) {
            return null;
        }

        Tuple insertResult = new Tuple(this.getTupleDesc());
        int records = 0;
        while(children[0].hasNext()) {
            try {
                Database.getBufferPool().insertTuple(new TransactionId(), this.tableId, children[0].next());
                records++;
            } catch (IOException e) {
                // TODO log this exception message
                e.printStackTrace();
            }
        }
        insertResult.setField(0, new IntField(records));

        return insertResult;
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
