package simpledb.operator;

import simpledb.Type;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * Project is an operator that implements a relational projection.
 */
public class Project extends Operator {

    private OpIterator child;
    private TupleDesc td;
    private ArrayList<Integer> outFieldIds;

    /**
     * Constructor accepts a child operator to read tuples to apply projection
     * to and a list of fields in output tuple
     *
     * @param fieldList The ids of the fields child's tupleDesc to project out
     * @param typesList the types of the fields in the final projection
     * @param child     The child operator
     */
    public Project(ArrayList<Integer> fieldList, ArrayList<Type> typesList,
                   OpIterator child) {
        this(fieldList, typesList.toArray(new Type[]{}), child);
    }

    public Project(ArrayList<Integer> fieldList, Type[] types,
                   OpIterator child) {
        this.child = child;
        outFieldIds = fieldList;
        String[] fieldAr = new String[fieldList.size()];
        TupleDesc childTupleDesc = child.getTupleDesc();

        for (int i = 0; i < fieldAr.length; i++) {
            fieldAr[i] = childTupleDesc.getFieldName(fieldList.get(i));
        }
        td = new TupleDesc(types, fieldAr);
    }

    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        child.open();
    }

    @Override
    public void close() {
        super.close();
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Operator.fetchNext implementation. Iterates over tuples from the child
     * operator, projecting out the fields from the tuple
     *
     * @return The next tuple, or null if there are no more tuples
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple t = child.next();
            Tuple newTuple = new Tuple(td);
            newTuple.setRecordId(t.getRecordId());
            for (int i = 0; i < td.numFields(); i++) {
                newTuple.setField(i, t.getField(outFieldIds.get(i)));
            }
            return newTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (this.child != children[0]) {
            this.child = children[0];
        }
    }

}
