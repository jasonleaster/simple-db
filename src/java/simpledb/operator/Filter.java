package simpledb.operator;

import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate predicate;
    private OpIterator opIterator;
    private TupleDesc tupleDesc;

    private OpIterator[] children;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.opIterator = child;
        this.tupleDesc = child.getTupleDesc();
        this.children = new OpIterator[1];
        this.children[0] = child;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        opIterator.open();
    }

    @Override
    public void close() {
        super.close();
        opIterator.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        opIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (opIterator.hasNext()) {
            Tuple tuple = opIterator.next();
            if (predicate.filter(tuple)) {
                return tuple;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return this.children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.children = children;
    }

}
