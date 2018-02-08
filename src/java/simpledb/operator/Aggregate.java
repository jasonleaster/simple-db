package simpledb.operator;

import simpledb.Type;
import simpledb.aggregator.AggregateFunc;
import simpledb.aggregator.Aggregator;
import simpledb.aggregator.IntegerAggregator;
import simpledb.aggregator.StringAggregator;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator[] children;
    private TupleDesc tupleDesc;
    private int afield;
    private int gfield;
    private AggregateFunc aop;
    private Aggregator aggregator;
    private OpIterator opIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, AggregateFunc aop) {
        this.children = new OpIterator[1];
        this.children[0] = child;
        this.tupleDesc = child.getTupleDesc();
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type fieldType;
        try {
            fieldType = this.tupleDesc.getFieldType(gfield);
        } catch (NoSuchElementException e) {
            fieldType = null;
        }


        if (this.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gfield, fieldType, afield, this.aop);
        } else {
            this.aggregator = new StringAggregator(gfield, fieldType, afield, this.aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return tupleDesc.getFieldName(gfield);
    }



    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return tupleDesc.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public AggregateFunc aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(AggregateFunc aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        this.children[0].open();
        while (children[0].hasNext()) {
            Tuple nextTuple = children[0].next();
            aggregator.mergeTupleIntoGroup(nextTuple);
        }
        this.opIterator = aggregator.iterator();
        this.opIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.opIterator.hasNext()) {
            return this.opIterator.next();
        } else {
            return null;
        }

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.children[0].rewind();
        this.opIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    @Override
    public void close() {
        super.close();
        this.children[0].close();
        this.opIterator.close();
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
