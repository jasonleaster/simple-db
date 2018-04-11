package simpledb.logical;

import simpledb.Catalog;
import simpledb.Database;
import simpledb.Debug;
import simpledb.JoinOptimizer;
import simpledb.TableStats;
import simpledb.Type;
import simpledb.aggregator.AggregateFunc;
import simpledb.aggregator.Aggregator;
import simpledb.exception.ParsingException;
import simpledb.field.Field;
import simpledb.field.IntField;
import simpledb.field.StringField;
import simpledb.operator.Aggregate;
import simpledb.operator.Filter;
import simpledb.operator.OpIterator;
import simpledb.operator.OrderBy;
import simpledb.operator.Predicate;
import simpledb.operator.Project;
import simpledb.operator.SeqScan;
import simpledb.transaction.TransactionId;
import simpledb.tuple.TupleDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * LogicalPlan represents a logical query plan that has been through
 * the parser and is ready to be processed by the optimizer.
 * <p>
 * A LogicalPlan consist of a collection of table scan nodes, join
 * nodes, filter nodes, a select list, and a group by field.
 * LogicalPlans can only represent queries with one aggregation field
 * and one group by field.
 * <p>
 * LogicalPlans can be converted to physical (optimized) plans using
 * the {@link #physicalPlan} method, which uses the
 * {@link JoinOptimizer} to order joins optimally and to select the
 * best implementations for joins.
 */
public class LogicalPlan {

    private static final String WILDCARD_CHAR = "*";

    private Vector<LogicalJoinNode> joins;

    /**
     * 逻辑计划中涉及的表
     */
    private Vector<LogicalScanNode> tables;

    private Vector<LogicalFilterNode> filters;
    private HashMap<String, OpIterator> subplanMap;
    private HashMap<String, Integer> tableMap;

    private Vector<LogicalSelectListNode> selectList;
    private String groupByField = null;
    private boolean hasAgg = false;
    private String aggOp;
    private String aggField;
    private boolean oByAsc, hasOrderBy = false;
    private String oByField;
    private String query;

    /**
     * Constructor -- generate an empty logical plan
     */
    public LogicalPlan() {
        this.joins = new Vector<>();
        this.filters = new Vector<>();
        this.tables = new Vector<>();
        this.subplanMap = new HashMap<>();
        this.tableMap = new HashMap<>();

        this.selectList = new Vector<>();
        this.query = "";
    }

    /**
     * Add a new filter to the logical plan
     *
     * @param field         The name of the over which the filter applies;
     *                      this can be a fully qualified field (tablename.field or alias.field),
     *                      or can be a unique field name without a tablename qualifier.If it is
     *                      an ambiguous name, it will throw a ParsingException
     * @param p             The predicate for the filter
     * @param constantValue the constant to compare the predicate against;
     *                      if field is an integer field, this should be a String representing
     *                      an integer
     * @throws ParsingException if field is not in one of the tables
     *                          added via {@link #addScan} or if field is ambiguous (e.g., two
     *                          tables contain a field named field.)
     */
    public void addFilter(String field, Predicate.Op p, String constantValue) throws ParsingException {

        field = this.disambiguateName(field);

        final String[] vals = field.split("[.]");
        final String tableName = vals[0];
        final String fieldName = vals[1];

        LogicalFilterNode node = new LogicalFilterNode(tableName, fieldName, p, constantValue);
        filters.addElement(node);
    }

    /**
     * Add a join between two fields of two different tables.
     *
     * @param joinField1 The name of the first join field; this can
     *                   be a fully qualified name (e.g., tableName.field or
     *                   alias.field) or may be an unqualified unique field name.  If
     *                   the name is ambiguous or unknown, a ParsingException will be
     *                   thrown.
     * @param joinField2 The name of the second join field
     * @param pred       The join predicate
     * @throws ParsingException if either of the fields is ambiguous,
     *                          or is not in one of the tables added via {@link #addScan}
     */

    public void addJoin(String joinField1, String joinField2, Predicate.Op pred) throws ParsingException {
        joinField1 = disambiguateName(joinField1);
        joinField2 = disambiguateName(joinField2);
        String table1Alias = joinField1.split("[.]")[0];
        String table2Alias = joinField2.split("[.]")[0];
        String pureField1 = joinField1.split("[.]")[1];
        String pureField2 = joinField2.split("[.]")[1];

        if (table1Alias.equals(table2Alias)) {
            throw new ParsingException("Cannot join on two fields from same table");
        }
        LogicalJoinNode lj = new LogicalJoinNode(table1Alias, table2Alias, pureField1, pureField2, pred);
        System.out.println("Added join between " + joinField1 + " and " + joinField2);
        joins.addElement(lj);

    }

    /**
     * Add a join between a field and a subquery.
     *
     * @param joinField1 The name of the first join field; this can
     *                   be a fully qualified name (e.g., tableName.field or
     *                   alias.field) or may be an unqualified unique field name.  If
     *                   the name is ambiguous or unknown, a ParsingException will be
     *                   thrown.
     * @param joinField2 the subquery to join with -- the join field
     *                   of the subquery is the first field in the result set of the query
     * @param pred       The join predicate.
     * @throws ParsingException if either of the fields is ambiguous,
     *                          or is not in one of the tables added via {@link #addScan}
     */
    public void addJoin(String joinField1, OpIterator joinField2, Predicate.Op pred) throws ParsingException {
        joinField1 = this.disambiguateName(joinField1);

        String table1 = joinField1.split("[.]")[0];
        String pureField = joinField1.split("[.]")[1];

        LogicalSubplanJoinNode lj = new LogicalSubplanJoinNode(table1, pureField, joinField2, pred);
        Debug.log("Added subplan join on " + joinField1);
        joins.addElement(lj);
    }

    /**
     * Add a scan to the plan. One scan node needs to be added for each alias of a table
     * accessed by the plan.
     *
     * @param table the id of the table accessed by the plan (can be resolved to a DbFile using {@link Catalog#getDatabaseFile}
     * @param name  the alias of the table in the plan
     */

    public void addScan(int table, String name) {
        Debug.log("Added scan of table " + name);
        tables.addElement(new LogicalScanNode(table, name));
        tableMap.put(name, table);
    }

    /**
     * Add a specified field/aggregate combination to the select list of the query.
     * Fields are output by the query such that the rightmost field is the first added via addProjectField.
     *
     * @param fieldName the field to add to the output
     * @param aggOp     the aggregate operation over the field.
     * @throws ParsingException
     */
    public void addProjectField(String fieldName, String aggOp) throws ParsingException {
        fieldName = this.disambiguateName(fieldName);
        if (WILDCARD_CHAR.equals(fieldName)) {
            fieldName = "null.*";
        }
        Debug.log("Added select list field " + fieldName);
        if (aggOp != null) {
            Debug.log("\t with aggregator " + aggOp);
        }
        selectList.addElement(new LogicalSelectListNode(aggOp, fieldName));
    }

    /**
     * Add an aggregate over the field with the specified grouping to
     * the query.  SimpleDb only supports a single aggregate
     * expression and GROUP BY field.
     *
     * @param op     the aggregation operator
     * @param aField the field to aggregate over
     * @param gField the field to group by
     * @throws ParsingException
     */
    public void addAggregate(String op, String aField, String gField) throws ParsingException {
        aField = this.disambiguateName(aField);
        if (gField != null) {
            gField = this.disambiguateName(gField);
        }
        aggOp = op;
        aggField = aField;
        groupByField = gField;
        hasAgg = true;
    }

    /**
     * Add an ORDER BY expression in the specified order on the specified field.  SimpleDb only supports
     * a single ORDER BY field.
     *
     * @param field the field to order by
     * @param asc   true if should be ordered in ascending order, false for descending order
     * @throws ParsingException
     */
    public void addOrderBy(String field, boolean asc) throws ParsingException {
        field = disambiguateName(field);
        oByField = field;
        oByAsc = asc;
        hasOrderBy = true;
    }

    /**
     * Given a name of a field, try to figure out what table it belongs to by looking
     * through all of the tables added via {@link #addScan}.
     *
     * @return A fully qualified name of the form tableAlias.name.  If the name parameter
     * is already qualified with a table name, simply returns name.
     * @throws ParsingException if the field cannot be found in any of the tables, or if the
     *                          field is ambiguous (appears in multiple tables)
     */
    private String disambiguateName(String name) throws ParsingException {
        if (name == null || name.isEmpty()) {
            throw new ParsingException("Empty String");
        }

        final String[] fields = name.split("[.]");
        if (fields.length == 2 && (!fields[0].equals("null"))) {
            return name;
        }
        if (fields.length > 2) {
            throw new ParsingException("Field " + name + " is not a valid field reference.");
        }
        if (fields.length == 2) {
            name = fields[1];
        }
        if (WILDCARD_CHAR.equals(name)) {
            return name;
        }

        /*
            now look for occurrences of name in all of the tables
         */
        final Iterator<LogicalScanNode> tableIt = tables.iterator();
        String tableName = null;
        while (tableIt.hasNext()) {
            LogicalScanNode table = tableIt.next();
            try {
                TupleDesc td = Database.getCatalog().getDatabaseFile(table.getTableId()).getTupleDesc();
                td.fieldNameToIndex(name);
                if (tableName == null) {
                    tableName = table.getAlias();
                } else {
                    throw new ParsingException("Field " + name + " appears in multiple tables; disambiguate by referring to it as tablename." + name);
                }
            } catch (NoSuchElementException e) {
                //ignore
            }
        }
        if (tableName != null) {
            return tableName + "." + name;
        } else {
            throw new ParsingException("Field " + name + " does not appear in any tables.");
        }
    }

    /**
     * Convert this LogicalPlan into a physicalPlan represented by a {@link OpIterator}.  Attempts to
     * find the optimal plan by using {@link JoinOptimizer#orderJoins} to order the joins in the plan.
     *
     * @param t              The transaction that the returned OpIterator will run as a part of
     * @param baseTableStats a HashMap providing a {@link TableStats}
     *                       object for each table used in the LogicalPlan.  This should
     *                       have one entry for each table referenced by the plan, not one
     *                       entry for each table alias (so a table tableId aliases as t1 and
     *                       t2 would have just one entry with key 'tableId' in this HashMap).
     * @param explain        flag indicating whether output visualizing the physical
     *                       query plan should be given.
     * @return A OpIterator representing this plan.
     * @throws ParsingException if the logical plan is not valid
     */
    public OpIterator physicalPlan(TransactionId t, Map<String, TableStats> baseTableStats, boolean explain) throws ParsingException {
        Iterator<LogicalScanNode> tableIt = tables.iterator();
        HashMap<String, String> equivMap = new HashMap<>();
        HashMap<String, Double> filterSelectivity = new HashMap<>();
        HashMap<String, TableStats> statsMap = new HashMap<>();

        while (tableIt.hasNext()) {
            LogicalScanNode table = tableIt.next();
            SeqScan ss;
            try {
                ss = new SeqScan(t, Database.getCatalog().getDatabaseFile(table.getTableId()).getId(), table.getAlias());
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown table " + table.getTableId());
            }

            subplanMap.put(table.getAlias(), ss);
            String baseTableName = Database.getCatalog().getTableName(table.getTableId());
            statsMap.put(baseTableName, baseTableStats.get(baseTableName));
            filterSelectivity.put(table.getAlias(), 1.0);
        }

        Iterator<LogicalFilterNode> filterIt = filters.iterator();
        while (filterIt.hasNext()) {
            LogicalFilterNode lf = filterIt.next();
            OpIterator subPlan = subplanMap.get(lf.getTableAlias());
            if (subPlan == null) {
                throw new ParsingException("Unknown table in WHERE clause " + lf.getTableAlias());
            }

            Type fieldType;
            TupleDesc td = subplanMap.get(lf.getTableAlias()).getTupleDesc();

            try {
                fieldType = td.getFieldType(td.fieldNameToIndex(lf.getFieldPureName()));
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field in filter expression " + lf.getFieldQuantifiedName());
            }
            Field field;
            if (fieldType == Type.INT_TYPE) {
                field = new IntField(Integer.parseInt(lf.getRightHandConstant()));
            } else {
                field = new StringField(lf.getRightHandConstant(), Type.STRING_LEN);
            }

            Predicate predicate;
            try {
                predicate = new Predicate(subPlan.getTupleDesc().fieldNameToIndex(lf.getFieldPureName()), lf.getPredicateOper(), field);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field " + lf.getFieldQuantifiedName());
            }
            subplanMap.put(lf.getTableAlias(), new Filter(predicate, subPlan));

            TableStats s = statsMap.get(Database.getCatalog().getTableName(this.getTableId(lf.getTableAlias())));

            double selectivity = s.estimateSelectivity(subPlan.getTupleDesc().fieldNameToIndex(lf.getFieldPureName()), lf.getPredicateOper(), field);
            filterSelectivity.put(lf.getTableAlias(), filterSelectivity.get(lf.getTableAlias()) * selectivity);

            //s.addSelectivityFactor(estimateFilterSelectivity(lf,statsMap));
        }

        JoinOptimizer jo = new JoinOptimizer(this, joins);

        joins = jo.orderJoins(statsMap, filterSelectivity, explain);

        Iterator<LogicalJoinNode> joinIt = joins.iterator();
        while (joinIt.hasNext()) {
            LogicalJoinNode lj = joinIt.next();
            OpIterator plan1;
            OpIterator plan2;
            boolean isSubQueryJoin = lj instanceof LogicalSubplanJoinNode;
            String t1name, t2name;

            if (equivMap.get(lj.t1Alias) != null) {
                t1name = equivMap.get(lj.t1Alias);
            } else {
                t1name = lj.t1Alias;
            }

            if (equivMap.get(lj.t2Alias) != null) {
                t2name = equivMap.get(lj.t2Alias);
            } else {
                t2name = lj.t2Alias;
            }

            plan1 = subplanMap.get(t1name);

            if (isSubQueryJoin) {
                plan2 = ((LogicalSubplanJoinNode) lj).getSubPlan();
                if (plan2 == null) {
                    throw new ParsingException("Invalid subquery.");
                }
            } else {
                plan2 = subplanMap.get(t2name);
            }

            if (plan1 == null) {
                throw new ParsingException("Unknown table in WHERE clause " + lj.t1Alias);
            }
            if (plan2 == null) {
                throw new ParsingException("Unknown table in WHERE clause " + lj.t2Alias);
            }

            OpIterator j;
            j = JoinOptimizer.instantiateJoin(lj, plan1, plan2);
            subplanMap.put(t1name, j);

            if (!isSubQueryJoin) {
                subplanMap.remove(t2name);
                equivMap.put(t2name, t1name);

                //keep track of the fact that this new node contains both tables
                //make sure anything that was equiv to lj.t2 (which we are just removed) is
                // marked as equiv to lj.t1 (which we are replacing lj.t2 with.)
                for (Map.Entry<String, String> s : equivMap.entrySet()) {
                    String val = s.getValue();
                    if (val.equals(t2name)) {
                        s.setValue(t1name);
                    }
                }

                // subplanMap.put(lj.t2, j);
            }

        }

        if (subplanMap.size() > 1) {
            throw new ParsingException("Query does not include join expressions joining all nodes!");
        }

        OpIterator node = subplanMap.entrySet().iterator().next().getValue();

        //walk the select list, to determine order in which to project output fields
        ArrayList<Integer> outFields = new ArrayList<>();
        ArrayList<Type> outTypes = new ArrayList<>();
        for (int i = 0; i < selectList.size(); i++) {
            LogicalSelectListNode si = selectList.elementAt(i);
            if (si.getAggregateOperator() != null) {
                outFields.add(groupByField != null ? 1 : 0);
                TupleDesc td = node.getTupleDesc();
                try {
                    td.fieldNameToIndex(si.getFieldName());
                } catch (NoSuchElementException e) {
                    throw new ParsingException("Unknown field " + si.getFieldName() + " in SELECT list");
                }
                //the type of all aggregate functions is INT
                outTypes.add(Type.INT_TYPE);
            } else if (hasAgg) {
                if (groupByField == null) {
                    throw new ParsingException("Field " + si.getFieldName() + " does not appear in GROUP BY list");
                }
                outFields.add(0);
                TupleDesc td = node.getTupleDesc();
                int id;
                try {
                    id = td.fieldNameToIndex(groupByField);
                } catch (NoSuchElementException e) {
                    throw new ParsingException("Unknown field " + groupByField + " in GROUP BY statement");
                }
                outTypes.add(td.getFieldType(id));
            } else if (si.getFieldName().equals("null." + WILDCARD_CHAR)) {
                TupleDesc td = node.getTupleDesc();
                for (i = 0; i < td.numFields(); i++) {
                    outFields.add(i);
                    outTypes.add(td.getFieldType(i));
                }
            } else {
                TupleDesc td = node.getTupleDesc();
                int id;
                try {
                    id = td.fieldNameToIndex(si.getFieldName());
                } catch (NoSuchElementException e) {
                    throw new ParsingException("Unknown field " + si.getFieldName() + " in SELECT list");
                }
                outFields.add(id);
                outTypes.add(td.getFieldType(id));

            }
        }

        if (hasAgg) {
            TupleDesc td = node.getTupleDesc();
            Aggregate aggNode;
            try {
                aggNode = new Aggregate(node,
                        td.fieldNameToIndex(aggField),
                        groupByField == null ? Aggregator.NO_GROUPING : td.fieldNameToIndex(groupByField),
                        AggregateFunc.getFuncByName(aggOp));
            } catch (NoSuchElementException | IllegalArgumentException e) {
                throw new ParsingException(e);
            }
            node = aggNode;
        }

        if (hasOrderBy) {
            node = new OrderBy(node.getTupleDesc().fieldNameToIndex(oByField), oByAsc, node);
        }

        return new Project(outFields, outTypes, node);
    }

    /**
     * Set the text of the query representing this logical plan.  Does NOT parse the
     * specified query -- this method is just used so that the object can print the
     * SQL it represents.
     *
     * @param query the text of the query associated with this plan
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * Get the query text associated with this plan via {@link #setQuery}.
     */
    public String getQuery() {
        return query;
    }

    /**
     * Given a table alias, return id of the table object (this id can be supplied to {@link Catalog#getDatabaseFile(int)}).
     * Aliases are added as base tables are added via {@link #addScan}.
     *
     * @param alias the table alias to return a table id for
     * @return the id of the table corresponding to alias, or null if the alias is unknown
     */
    public Integer getTableId(String alias) {
        return tableMap.get(alias);
    }

    public HashMap<String, Integer> getTableAliasToIdMapping() {
        return this.tableMap;
    }

}
