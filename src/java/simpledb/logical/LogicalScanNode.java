package simpledb.logical;

import simpledb.Catalog;

/**
 * A LogicalScanNode represents table in the FROM list in a
 * LogicalQueryPlan
 * */
public class LogicalScanNode {

    /**
     * The name (alias) of the table as it is used in the query
     * */
    private String alias;

    /**
     * The table identifier (can be passed to {@link Catalog#getDatabaseFile})
     * to retrieve a DbFile
     * */
    private int tableId;

    public LogicalScanNode(int table, String tableAlias) {
        this.alias = tableAlias;
        this.tableId = table;
    }

    public String getAlias() {
        return alias;
    }

    public int getTableId() {
        return tableId;
    }
}

