package simpledb;

import simpledb.field.Field;
import simpledb.operator.Predicate;

import java.io.Serializable;

/**
 * IndexPredicate compares a field which has index on it against a given value
 * @see IndexOpIterator
 */
public class IndexPredicate {
	
    private Predicate.Op op;
    private Field field;

    /**
     * Constructor.
     *
     * @param field The value that the predicate compares against.
     * @param op The operation to apply (as defined in Predicate.Op); either
     *   Predicate.AggregateFunc.GREATER_THAN, Predicate.AggregateFunc.LESS_THAN, Predicate.AggregateFunc.EQUAL,
     *   Predicate.AggregateFunc.GREATER_THAN_OR_EQ, or Predicate.AggregateFunc.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public IndexPredicate(Predicate.Op op, Field field) {
        this.op = op;
        this.field = field;
    }

    public Field getField() {
        return field;
    }

    public Predicate.Op getOp() {
        return op;
    }

    /**
     * Return true if the field in the supplied predicate
     * is satisfied by this predicate's field and operator.
     *
     * @param ipd The field to compare against.
    */
    public boolean equals(IndexPredicate ipd) {
        if (ipd == null) {
            return false;
        }
        return (op.equals(ipd.op) && field.equals(ipd.field));
    }

}
