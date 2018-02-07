package simpledb.logical;

import simpledb.operator.Predicate;

/** A LogicalFilterNode represents the parameters of a filter in the WHERE clause of a query.
    <p>
    Filter is of the form t.f p c
    <p>
    Where t is a table, f is a field in t, p is a predicate, and c is a constant
*/
public class LogicalFilterNode {

    /**
     * The alias of a table (or the name if no alias) over which the filter ranges
     */
    private String tableAlias;

    /**
     * The predicate in the filter
     */
    private Predicate.Op predicateOper;

    /**
     * The constant on the right side of the filter
     * */
    private String rightHandConstant;

    /**
     * The field from tableId which is in the filter. The pure name, without alias or tablename
     */
    private String fieldPureName;

    private String fieldQuantifiedName;

    public LogicalFilterNode(String table, String field, Predicate.Op pred, String constant) {
        this.tableAlias = table;
        this.predicateOper = pred;
        this.rightHandConstant = constant;
        String[] tmps = field.split("[.]");
        if (tmps.length > 1) {
            fieldPureName = tmps[tmps.length - 1];
        } else {
            fieldPureName = field;
        }
        this.fieldQuantifiedName = tableAlias + "." + fieldPureName;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public Predicate.Op getPredicateOper() {
        return predicateOper;
    }

    public String getRightHandConstant() {
        return rightHandConstant;
    }

    public String getFieldPureName() {
        return fieldPureName;
    }

    public String getFieldQuantifiedName() {
        return fieldQuantifiedName;
    }
}
