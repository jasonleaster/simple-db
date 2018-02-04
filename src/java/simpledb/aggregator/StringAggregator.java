package simpledb.aggregator;

import simpledb.exception.DbException;
import simpledb.Type;
import simpledb.exception.TransactionAbortedException;
import simpledb.field.Field;
import simpledb.field.IntField;
import simpledb.operator.OpIterator;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private AggregateFunc what;

    /**
     * GroupFiled -> result
     */
    private Map<Field, Tuple> results;

    private TupleDesc tupleDescForAggregatorResult;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple,
     *                or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE),
     *                    or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, AggregateFunc what) {
        if (what != AggregateFunc.COUNT) {
            throw new IllegalArgumentException("The parameter of StringAggregator ERROR!");
        }

        this.gbfield = gbfield;
        this.what = what;
        this.results = new ConcurrentHashMap<>();

        Type[] types = new Type[2];
        String[] names = new String[2];
        types[0] = Type.INT_TYPE;
        types[1] = Type.INT_TYPE;
        names[0] = "groupVal";
        names[1] = "aggregateVal";
        this.tupleDescForAggregatorResult = new TupleDesc(types, names);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tuple the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tuple) {
        Field curGrpByField = tuple.getField(this.gbfield);

        IntField after = null;
        if (!this.results.containsKey(curGrpByField)) {
            after = new IntField(1);
        } else {
            Tuple preResult = this.results.get(curGrpByField);
            IntField preAgField = (IntField) preResult.getField(1);
            switch (what){
                case COUNT:
                    after = new IntField(preAgField.getValue() + 1);
                    break;
                default:
                    break;
            }
        }

        if (after != null) {
            Tuple history = new Tuple(this.tupleDescForAggregatorResult);
            history.setField(0, curGrpByField);
            history.setField(1, after);
            this.results.put(curGrpByField, history);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        return new StringAgOpIterator();
    }

    private class StringAgOpIterator implements OpIterator {
        private boolean opened = false;
        private Iterator<Map.Entry<Field, Tuple>> iter;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = results.entrySet().iterator();
            opened = true;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (opened && iter.hasNext()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iter.next().getValue();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDescForAggregatorResult;
        }

        @Override
        public void close() {

        }
    }

}
