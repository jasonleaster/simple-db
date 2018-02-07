package simpledb.logical;

/**
 * A LogicalSelectListNode represents a clause in the select list in
 * a LogicalQueryPlan
*/
public class LogicalSelectListNode {
    /**
     * The field name being selected; the name may be (optionally) be
     * qualified with a table name or alias.
     */
    private String fieldName;
   
    /**
     * The aggregation operation over the field (if any)
     * */
    private String aggregateOperator;

    public LogicalSelectListNode(String aggregateOperator, String fieldName) {
        this.aggregateOperator = aggregateOperator;
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getAggregateOperator() {
        return aggregateOperator;
    }
}
