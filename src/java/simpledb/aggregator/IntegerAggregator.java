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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final IntField EMPTY_FIELD = new IntField(-1);

    private int gbField;
    private Type gbFieldType;
    private int aField;
    private AggregateFunc what;

    /**
     * GroupFiled -> totalSum
     */
    private Map<Field, Integer> totalSum;

    /**
     * GroupFiled -> totalCounts
     */
    private Map<Field, Integer> totalCounts;

    /**
     * GroupFiled -> result
     */
    private Map<Field, Tuple> results;

    private TupleDesc tupleDescForAggregatorResult;


    /**
     * Aggregate constructor
     * 
     * @param gbField
     *            the 0-based index of the group-by field in the result, or
     *            NO_GROUPING if there is no grouping
     * @param gbFieldType
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param aField
     *            the 0-based index of the aggregate field in the result
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbField, Type gbFieldType, int aField, AggregateFunc what) {
        this.gbField = gbField;
        this.gbFieldType = gbFieldType;
        this.aField = aField;
        this.what = what;

        this.totalCounts = new ConcurrentHashMap<>();
        this.totalSum    = new ConcurrentHashMap<>();
        this.results = new ConcurrentHashMap<>();

        if (this.gbField >= 0) {
            Type[] types = new Type[2];
            String[] names = new String[2];
            types[0] = this.gbFieldType;
            types[1] = Type.INT_TYPE;
            names[0] = "groupVal";
            names[1] = "aggregateVal";
            this.tupleDescForAggregatorResult = new TupleDesc(types, names);
        } else {
            Type[] types = new Type[1];
            String[] names = new String[1];
            types[0] = Type.INT_TYPE;
            names[0] = "aggregateVal";
            this.tupleDescForAggregatorResult = new TupleDesc(types, names);
        }

    }

    /**
     * Merge a new result into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tuple
     *            the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tuple) {

        Field curGrpByField;
        if (this.gbField < 0) {
            curGrpByField = EMPTY_FIELD;
        } else {
            curGrpByField = tuple.getField(this.gbField);
            if (curGrpByField.getType() != this.gbFieldType) {
                return;
            }
        }

        IntField curAgField = (IntField) tuple.getField(this.aField);

        if (totalSum.containsKey(curGrpByField)) {
            totalSum.put(curGrpByField, totalSum.get(curGrpByField) + curAgField.getValue());
        } else {
            totalSum.put(curGrpByField, curAgField.getValue());
        }

        if (totalCounts.containsKey(curGrpByField)) {
            totalCounts.put(curGrpByField, totalCounts.get(curGrpByField) + 1);
        } else {
            totalCounts.put(curGrpByField, 1);
        }

        IntField after = null;
        if (!this.results.containsKey(curGrpByField)) {
            after = curAgField;
            if (what == AggregateFunc.COUNT) {
                after = new IntField(1);
            }
        } else {
            Tuple preResult = this.results.get(curGrpByField);
            // 拿到一个字段，即Aggregate的值
            final int indexForAgVal = preResult.getTupleDesc().numFields() - 1;
            IntField preAgField = (IntField) preResult.getField(indexForAgVal);
            switch (what){
                case MAX:
                    after = preAgField.getValue() < curAgField.getValue() ? curAgField : preAgField;
                    break;
                case MIN:
                    after = preAgField.getValue() < curAgField.getValue() ? preAgField : curAgField;
                    break;
                case COUNT:
                    after = new IntField(preAgField.getValue() + 1);
                    break;
                case SUM:
                    after = new IntField(preAgField.getValue() + curAgField.getValue());
                    break;
                case AVG:
                    after = new IntField(totalSum.get(curGrpByField) / totalCounts.get(curGrpByField)); // 会有精度损失
                    break;
                case SUM_COUNT: break;
                case SC_AVG: break;
                default:
                    break;
            }
        }

        if (after != null) {
            Tuple history = new Tuple(this.tupleDescForAggregatorResult);
            if (this.gbField >= 0) {

                history.setField(0, curGrpByField);
                history.setField(1, after);
                this.results.put(curGrpByField, history);
            } else {
                history.setField(0, after);
                this.results.put(curGrpByField, history);
            }

        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    @Override
    public OpIterator iterator() {
        return new IntegerAgOpIterator();
    }

    private class IntegerAgOpIterator implements OpIterator {
        private boolean opened = false;
        private Iterator<Map.Entry<Field, Tuple>> iter;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = results.entrySet().iterator();
            opened = true;
        }

        @Override
        public void close() {
            opened = false;
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
    }

}
