package simpledb.aggregator;

import simpledb.operator.OpIterator;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleIterator;

import java.io.Serializable;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    int NO_GROUPING = -1;

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @see TupleIterator for a possible helper
     */
    OpIterator iterator();
}
