package simpledb.operator;

import simpledb.field.Field;
import simpledb.tuple.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private Predicate.Op op;
    private int fieldIndex1;
    private int fieldIndex2;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.AggregateFunc); either
     *               Predicate.AggregateFunc.GREATER_THAN, Predicate.AggregateFunc.LESS_THAN,
     *               Predicate.AggregateFunc.EQUAL, Predicate.AggregateFunc.GREATER_THAN_OR_EQ, or
     *               Predicate.AggregateFunc.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        this.fieldIndex1 = field1;
        this.fieldIndex2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        if (t1 == null || t2 == null) {
            return false;
        }

        Field field1 = t1.getField(fieldIndex1);
        Field field2 = t2.getField(fieldIndex2);

        return field1.compare(op, field2);
    }

    public int getField1() {
        return fieldIndex1;
    }

    public int getField2() {
        return fieldIndex2;
    }

    public Predicate.Op getOperator() {
        return op;
    }
}
