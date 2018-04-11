package simpledb.operator;

import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.field.Field;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * OrderBy is an operator that implements a relational ORDER BY.
 */
public class OrderBy extends Operator {

    private OpIterator child;
    private TupleDesc td;
    private ArrayList<Tuple> childTups = new ArrayList<Tuple>();
    private int orderByField;
    private String orderByFieldName;
    private Iterator<Tuple> it;
    private boolean asc;

    /**
     * Creates a new OrderBy node over the tuples from the iterator.
     * 
     * @param orderByField
     *            the field to which the sort is applied.
     * @param asc
     *            true if the sort order is ascending.
     * @param child
     *            the tuples to sort.
     */
    public OrderBy(int orderByField, boolean asc, OpIterator child) {
        this.child = child;
        td = child.getTupleDesc();
        this.orderByField = orderByField;
        this.orderByFieldName = td.getFieldName(orderByField);
        this.asc = asc;
    }
    
    public boolean isASC()
    {
	return this.asc;
    }
    
    public int getOrderByField()
    {
        return this.orderByField;
    }
    
    public String getOrderFieldName()
    {
	return this.orderByFieldName;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        // load all the tuples in a collection, and sort it
        while (child.hasNext()) {
            childTups.add((Tuple) child.next());
        }
        Collections.sort(childTups, new TupleComparator(orderByField, asc));
        it = childTups.iterator();
        super.open();
    }

    @Override
    public void close() {
        super.close();
        it = null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        it = childTups.iterator();
    }

    /**
     * Operator.fetchNext implementation. Returns tuples from the child operator
     * in order
     * 
     * @return The next tuple in the ordering, or null if there are no more
     *         tuples
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if (it != null && it.hasNext()) {
            return it.next();
        } else {
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

    class TupleComparator implements Comparator<Tuple> {
        private int field;
        private boolean asc;

        public TupleComparator(int field, boolean asc) {
            this.field = field;
            this.asc = asc;
        }

        @Override
        public int compare(Tuple o1, Tuple o2) {
            Field t1 = (o1).getField(field);
            Field t2 = (o2).getField(field);

            if (t1.compare(Predicate.Op.EQUALS, t2)) {
                return 0;
            }
            if (t1.compare(Predicate.Op.GREATER_THAN, t2)) {
                return asc ? 1 : -1;
            }
            else {
                return asc ? -1 : 1;
            }
        }
    }
}
