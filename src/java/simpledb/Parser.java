package simpledb;

import Zql.ZConstant;
import Zql.ZDelete;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import Zql.ZGroupBy;
import Zql.ZInsert;
import Zql.ZOrderBy;
import Zql.ZQuery;
import Zql.ZSelectItem;
import Zql.ZStatement;
import Zql.ZTransactStmt;
import Zql.ZqlParser;
import jline.ArgumentCompletor;
import jline.SimpleCompletor;
import simpledb.exception.DbException;
import simpledb.exception.ParsingException;
import simpledb.exception.TransactionAbortedException;
import simpledb.field.IntField;
import simpledb.field.StringField;
import simpledb.logical.LogicalPlan;
import simpledb.operator.Delete;
import simpledb.operator.Insert;
import simpledb.operator.OpIterator;
import simpledb.operator.Operator;
import simpledb.operator.Predicate;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Vector;

/**
 * SQL语句解析器
 */
public class Parser {

    private static boolean explain = false;

    /**
     * 是否通过命令行交互
     */
    private boolean interactive = true;

    /**
     * 当前事务
     */
    private Transaction transaction = null;

    /**
     * 标记当前事务是否是用户自定义的事务
     */
    private boolean inUserTrans = false;

    /**
     * Basic SQL completions
     */
    private static final String[] SQL_COMMANDS = {"select", "from", "where",
            "group by", "max(", "min(", "avg(", "count", "rollback", "commit",
            "insert", "delete", "values", "into"};

    private static Predicate.Op getOp(String s) throws ParsingException {
        if (s.equals("=")) {
            return Predicate.Op.EQUALS;
        }
        if (s.equals(">")) {
            return Predicate.Op.GREATER_THAN;
        }
        if (s.equals(">=")) {
            return Predicate.Op.GREATER_THAN_OR_EQ;
        }
        if (s.equals("<")) {
            return Predicate.Op.LESS_THAN;
        }
        if (s.equals("<=")) {
            return Predicate.Op.LESS_THAN_OR_EQ;
        }
        if (s.equals("LIKE")) {
            return Predicate.Op.LIKE;
        }
        if (s.equals("~")) {
            return Predicate.Op.LIKE;
        }
        if (s.equals("<>")) {
            return Predicate.Op.NOT_EQUALS;
        }
        if (s.equals("!=")) {
            return Predicate.Op.NOT_EQUALS;
        }

        throw new ParsingException("Unknown predicate " + s);
    }

    private void processExpression(TransactionId tid, ZExpression wx, LogicalPlan lp)
            throws ParsingException {
        if (wx.getOperator().equals("AND")) {
            for (int i = 0; i < wx.nbOperands(); i++) {
                if (!(wx.getOperand(i) instanceof ZExpression)) {
                    throw new ParsingException("Nested queries are currently unsupported.");
                }

                ZExpression newWx = (ZExpression) wx.getOperand(i);
                processExpression(tid, newWx, lp);
            }
        } else if (wx.getOperator().equals("OR")) {
            throw new ParsingException("OR expressions currently unsupported.");
        } else {
            // this is a binary expression comparing two constants
            @SuppressWarnings("unchecked")
            Vector<ZExp> ops = wx.getOperands();
            if (ops.size() != 2) {
                throw new ParsingException("Only simple binary expressions of the form A op B are currently supported.");
            }

            boolean isJoin = false;
            Predicate.Op op = getOp(wx.getOperator());

            // otherwise is a Query
            boolean op1const = ops.elementAt(0) instanceof ZConstant;
            boolean op2const = ops.elementAt(1) instanceof ZConstant;

            if (op1const && op2const) {
                isJoin = ((ZConstant) ops.elementAt(0)).getType() == ZConstant.COLUMNNAME
                        && ((ZConstant) ops.elementAt(1)).getType() == ZConstant.COLUMNNAME;
            } else if (ops.elementAt(0) instanceof ZQuery
                    || ops.elementAt(1) instanceof ZQuery) {
                isJoin = true;
            } else if (ops.elementAt(0) instanceof ZExpression
                    || ops.elementAt(1) instanceof ZExpression) {
                throw new ParsingException("Only simple binary expressions of the form A op B are currently supported, where A or B are fields, constants, or sub-queries.");
            } else {
                isJoin = false;
            }

            /*
                join node
             */
            if (isJoin) {
                String tab1field = "";
                String tab2field;

                if (!op1const) {
                    /*
                        left op is a nested query
                        generate a virtual table for the left op
                        this isn't a valid ZQL query
                     */
                } else {
                    tab1field = ((ZConstant) ops.elementAt(0)).getValue();
                }

                if (!op2const) {
                    /*
                        right op is a nested query
                     */
                    LogicalPlan sublp = parseQueryLogicalPlan(tid,
                            (ZQuery) ops.elementAt(1));
                    OpIterator pp = sublp.physicalPlan(tid,
                            TableStats.getStatsMap(), explain);
                    lp.addJoin(tab1field, pp, op);
                } else {
                    tab2field = ((ZConstant) ops.elementAt(1)).getValue();
                    lp.addJoin(tab1field, tab2field, op);
                }

            } else { // select node
                String column;
                String compValue;
                ZConstant op1 = (ZConstant) ops.elementAt(0);
                ZConstant op2 = (ZConstant) ops.elementAt(1);
                if (op1.getType() == ZConstant.COLUMNNAME) {
                    column = op1.getValue();
                    compValue = new String(op2.getValue());
                } else {
                    column = op2.getValue();
                    compValue = new String(op1.getValue());
                }

                lp.addFilter(column, op, compValue);

            }
        }

    }

    /**
     * 构建逻辑查询计划
     */
    private LogicalPlan parseQueryLogicalPlan(TransactionId tid, ZQuery query)
            throws ParsingException {
        @SuppressWarnings("unchecked")
        Vector<ZFromItem> from = query.getFrom();
        LogicalPlan logicalPlan = new LogicalPlan();
        logicalPlan.setQuery(query.toString());
        // walk through tables in the FROM clause
        for (int i = 0; i < from.size(); i++) {
            ZFromItem fromIt = from.elementAt(i);
            try {
                // will fall through if table doesn't exist
                int id = Database.getCatalog().getTableId(fromIt.getTable());
                String name;

                if (fromIt.getAlias() != null) {
                    name = fromIt.getAlias();
                } else {
                    name = fromIt.getTable();
                }

                logicalPlan.addScan(id, name);

                // XXX handle subquery?
            } catch (NoSuchElementException e) {
                e.printStackTrace();
                throw new ParsingException("Table " + fromIt.getTable() + " is not in catalog");
            }
        }

        // now parse the where clause, creating Filter and Join nodes as needed
        ZExp w = query.getWhere();
        if (w != null) {
            if (!(w instanceof ZExpression)) {
                throw new ParsingException("Nested queries are currently unsupported.");
            }
            ZExpression wx = (ZExpression) w;
            processExpression(tid, wx, logicalPlan);
        }

        // now look for group by fields
        ZGroupBy gby = query.getGroupBy();
        String groupByField = null;
        if (gby != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> gbs = gby.getGroupBy();
            if (gbs.size() > 1) {
                throw new ParsingException("At most one grouping field expression supported.");
            }
            if (gbs.size() == 1) {
                ZExp gbe = gbs.elementAt(0);
                if (!(gbe instanceof ZConstant)) {
                    throw new ParsingException("Complex grouping expressions (" + gbe + ") not supported.");
                }
                groupByField = ((ZConstant) gbe).getValue();
                System.out.println("GROUP BY FIELD : " + groupByField);
            }

        }

        // walk the select list, pick out aggregates, and check for query
        // validity
        @SuppressWarnings("unchecked")
        Vector<ZSelectItem> selectList = query.getSelect();
        String aggField = null;
        String aggFun = null;

        for (int i = 0; i < selectList.size(); i++) {
            ZSelectItem si = selectList.elementAt(i);
            if (si.getAggregate() == null
                    && (si.isExpression() && !(si.getExpression() instanceof ZConstant))) {
                throw new ParsingException("Expressions in SELECT list are not supported.");
            }
            if (si.getAggregate() != null) {
                if (aggField != null) {
                    throw new ParsingException(
                            "Aggregates over multiple fields not supported.");
                }
                aggField = ((ZConstant) ((ZExpression) si.getExpression())
                        .getOperand(0)).getValue();
                aggFun = si.getAggregate();
                System.out.println("Aggregate field is " + aggField
                        + ", agg fun is : " + aggFun);
                logicalPlan.addProjectField(aggField, aggFun);
            } else {
                if (groupByField != null
                        && !(groupByField.equals(si.getTable() + "."
                        + si.getColumn()) || groupByField.equals(si
                        .getColumn()))) {
                    throw new ParsingException("Non-aggregate field "
                            + si.getColumn()
                            + " does not appear in GROUP BY list.");
                }
                logicalPlan.addProjectField(si.getTable() + "." + si.getColumn(), null);
            }
        }

        if (groupByField != null && aggFun == null) {
            throw new ParsingException("GROUP BY without aggregation.");
        }

        if (aggFun != null) {
            logicalPlan.addAggregate(aggFun, aggField, groupByField);
        }
        // sort the data

        if (query.getOrderBy() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZOrderBy> obys = query.getOrderBy();
            if (obys.size() > 1) {
                throw new ParsingException("Multi-attribute ORDER BY is not supported.");
            }
            ZOrderBy oby = obys.elementAt(0);
            if (!(oby.getExpression() instanceof ZConstant)) {
                throw new ParsingException("Complex ORDER BY's are not supported");
            }
            ZConstant f = (ZConstant) oby.getExpression();

            logicalPlan.addOrderBy(f.getValue(), oby.getAscOrder());

        }
        return logicalPlan;
    }

    private Query handleQueryStatement(ZQuery s, TransactionId tId)
            throws ParsingException {

        Query query = new Query(tId);

        LogicalPlan lp = parseQueryLogicalPlan(tId, s);
        OpIterator physicalPlan = lp.physicalPlan(tId, TableStats.getStatsMap(), explain);

        query.setPhysicalPlan(physicalPlan);
        query.setLogicalPlan(lp);

        if (physicalPlan != null) {
            Class<?> clazz;
            try {
                clazz = Class.forName("simpledb.OperatorCardinality");
                Class<?> p = Operator.class;
                Class<?> h = Map.class;

                java.lang.reflect.Method m = clazz.getMethod("updateOperatorCardinality", p, h, h);

                System.out.println("The query plan is:");

                m.invoke(null, physicalPlan,
                        lp.getTableAliasToIdMapping(), TableStats.getStatsMap());

                clazz = Class.forName("simpledb.QueryPlanVisualizer");

                m = clazz.getMethod("printQueryPlanTree",
                        OpIterator.class, System.out.getClass());

                m.invoke(clazz.newInstance(), physicalPlan, System.out);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return query;
    }

    private Query handleInsertStatement(ZInsert s, TransactionId tId)
            throws DbException, ParsingException {
        int tableId;
        try {
            // will fall through if table doesn't exist
            tableId = Database.getCatalog().getTableId(s.getTable());
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown table : " + s.getTable());
        }

        TupleDesc td = Database.getCatalog().getTupleDesc(tableId);

        Tuple tuple = new Tuple(td);
        int i = 0;
        OpIterator newTups;

        if (s.getValues() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> values = (Vector<ZExp>) s.getValues();
            if (td.numFields() != values.size()) {
                throw new ParsingException(
                        "INSERT statement does not contain same number of fields as table "
                                + s.getTable());
            }
            for (ZExp e : values) {

                if (!(e instanceof ZConstant)) {
                    throw new ParsingException("Complex expressions not allowed in INSERT statements.");
                }
                ZConstant zc = (ZConstant) e;
                if (zc.getType() == ZConstant.NUMBER) {
                    if (td.getFieldType(i) != Type.INT_TYPE) {
                        throw new ParsingException("Value " + zc.getValue() + " is not an integer, expected a string.");
                    }
                    IntField f = new IntField(new Integer(zc.getValue()));
                    tuple.setField(i, f);
                } else if (zc.getType() == ZConstant.STRING) {
                    if (td.getFieldType(i) != Type.STRING_TYPE) {
                        throw new ParsingException("Value "
                                + zc.getValue()
                                + " is a string, expected an integer.");
                    }
                    StringField f = new StringField(zc.getValue(), Type.STRING_LEN);
                    tuple.setField(i, f);
                } else {
                    throw new ParsingException("Only string or int fields are supported.");
                }
                i++;
            }
            ArrayList<Tuple> tups = new ArrayList<Tuple>();
            tups.add(tuple);
            newTups = new TupleArrayIterator(tups);

        } else {
            ZQuery zq = s.getQuery();
            LogicalPlan lp = parseQueryLogicalPlan(tId, zq);
            newTups = lp.physicalPlan(tId, TableStats.getStatsMap(), explain);
        }
        Query insertQ = new Query(tId);
        insertQ.setPhysicalPlan(new Insert(tId, newTups, tableId));
        return insertQ;
    }

    private Query handleDeleteStatement(ZDelete s, TransactionId tid)
            throws ParsingException {
        int tableId;
        try {
            // will fall through if table doesn't exist
            tableId = Database.getCatalog().getTableId(s.getTable());
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown table : " + s.getTable());
        }
        String name = s.getTable();
        Query sdbq = new Query(tid);

        LogicalPlan lp = new LogicalPlan();
        lp.setQuery(s.toString());

        lp.addScan(tableId, name);
        if (s.getWhere() != null) {
            processExpression(tid, (ZExpression) s.getWhere(), lp);
        }
        lp.addProjectField("null.*", null);

        OpIterator op = new Delete(tid, lp.physicalPlan(tid,
                TableStats.getStatsMap(), false));
        sdbq.setPhysicalPlan(op);

        return sdbq;

    }

    private void handleTransactStatement(ZTransactStmt s) throws IOException, ParsingException {

        if (s.getStmtType().equals("COMMIT")) {
            if (transaction == null) {
                throw new ParsingException("No transaction is currently running");
            }

            transaction.commit();
            transaction = null;
            inUserTrans = false;

            System.out.println("Transaction " + transaction.getId().getId() + " committed.");

        } else if (s.getStmtType().equals("ROLLBACK")) {
            if (transaction == null) {
                throw new ParsingException("No transaction is currently running");
            }
            transaction.abort();
            System.out.println("Transaction " + transaction.getId().getId() + " aborted.");
            transaction = null;
            inUserTrans = false;
        } else if (s.getStmtType().equals("SET TRANSACTION")) {
            if (transaction != null) {
                throw new ParsingException("Can't start new transactions until current transaction has been committed or rolledback.");
            }
            transaction = new Transaction();
            transaction.start();
            inUserTrans = true;
            System.out.println("Started a new transaction tid = " + transaction.getId().getId());
        } else {
            throw new ParsingException("Unsupported operation");
        }
    }

    public LogicalPlan generateLogicalPlan(TransactionId tid, String s)
            throws ParsingException {
        ByteArrayInputStream bis = new ByteArrayInputStream(s.getBytes());
        ZqlParser p = new ZqlParser(bis);
        try {
            ZStatement stmt = p.readStatement();
            if (stmt instanceof ZQuery) {
                LogicalPlan lp = parseQueryLogicalPlan(tid, (ZQuery) stmt);
                return lp;
            }
        } catch (Zql.ParseException e) {
            throw new ParsingException("Invalid SQL expression: \n \t " + e);
        }

        throw new ParsingException("Cannot generate logical plan for expression : " + s);
    }

    public void processNextStatement(String s) {
        try {
            processNextStatement(new ByteArrayInputStream(s.getBytes("UTF-8")));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void processNextStatement(InputStream is) {
        try {
            ZqlParser p = new ZqlParser(is);
            ZStatement s = p.readStatement();

            Query query = null;
            if (s instanceof ZTransactStmt) {
                handleTransactStatement((ZTransactStmt) s);
            } else {
                if (!this.inUserTrans) {
                    transaction = new Transaction();
                    transaction.start();
                    System.out.println("Started a new transaction tid = " + transaction.getId().getId());
                }
                try {
                    if (s instanceof ZInsert) {
                        query = handleInsertStatement((ZInsert) s, transaction.getId());
                    } else if (s instanceof ZDelete) {
                        query = handleDeleteStatement((ZDelete) s, transaction.getId());
                    } else if (s instanceof ZQuery) {
                        query = handleQueryStatement((ZQuery) s, transaction.getId());
                    } else {
                        System.out.println("Can't parse " + s
                                + "\n -- parser only handles SQL transactions, insert, delete, and select statements");
                    }
                    if (query != null) {
                        query.execute();
                    }

                    if (!inUserTrans && transaction != null) {
                        transaction.commit();
                        System.out.println("Transaction " + transaction.getId().getId() + " committed.");
                    }
                } catch (Throwable a) {
                    // Whenever error happens, abort the current transaction
                    if (transaction != null) {
                        transaction.abort();
                        System.out.println("Transaction " + transaction.getId().getId()
                                + " aborted because of unhandled error");
                    }
                    this.inUserTrans = false;

                    if (a instanceof ParsingException || a instanceof Zql.ParseException) {
                        throw new ParsingException((Exception) a);
                    }
                    if (a instanceof Zql.TokenMgrError) {
                        throw (Zql.TokenMgrError) a;
                    }
                    throw new DbException(a.getMessage());
                } finally {
                    if (!inUserTrans) {
                        transaction = null;
                    }
                }
            }
        } catch (DbException | IOException e) {
            e.printStackTrace();
        } catch (ParsingException | Zql.ParseException | Zql.TokenMgrError e) {
            System.out.println("Invalid SQL expression: \n \t" + e.getMessage());
        }
    }

    static final String usage = "Usage: parser catalogFile [-explain] [-f queryFile]";
    static final int SLEEP_TIME = 1000;

    protected void shutdown() {
        System.out.println("Bye");
    }

    private void start(String[] argv) throws IOException {
        // first add tables to database
        Database.getCatalog().loadSchema(argv[0]);
        TableStats.computeStatistics();

        String queryFile = null;

        if (argv.length > 1) {
            for (int i = 1; i < argv.length; i++) {
                if (argv[i].equals("-explain")) {
                    explain = true;
                    System.out.println("Explain mode enabled.");
                } else if (argv[i].equals("-f")) {
                    interactive = false;
                    if (i++ == argv.length) {
                        System.out.println("Expected file name after -f\n" + usage);
                        System.exit(0);
                    }
                    queryFile = argv[i];
                } else {
                    System.out.println("Unknown argument " + argv[i] + "\n " + usage);
                }
            }
        }
        if (!interactive) {
            try {
                // curtrans = new Transaction();
                // curtrans.start();
                try {
                    Thread.sleep(SLEEP_TIME);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                long startTime = System.currentTimeMillis();
                processNextStatement(new FileInputStream(new File(queryFile)));
                long time = System.currentTimeMillis() - startTime;
                System.out.printf("----------------\n%.2f seconds\n\n", ((double) time / 1000.0));
                System.out.println("Press Enter to exit");
                System.in.read();
                this.shutdown();
            } catch (FileNotFoundException e) {
                System.out.println("Unable to find query file" + queryFile);
                e.printStackTrace();
            }
        } else {
            /*
                no query file, run interactive prompt
                TODO ConsoleReader 用不了了，搞定这个问题，尝试提供自动补全功能
             */
            //ConsoleReader reader = new ConsoleReader();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            // Add really stupid tab completion for simple SQL
            ArgumentCompletor completor = new ArgumentCompletor(new SimpleCompletor(SQL_COMMANDS));
            completor.setStrict(false); // match at any position
            //reader.addCompletor(completor);

            StringBuilder buffer = new StringBuilder();
            String line;
            boolean quit = false;
            System.out.print("SimpleDB> ");
            while (!quit && (line = reader.readLine()) != null) {
                // Split statements at ';': handles multiple statements on one
                // line, or one
                // statement spread across many lines
                while (line.indexOf(';') >= 0) {
                    int split = line.indexOf(';');
                    buffer.append(line.substring(0, split + 1));
                    String cmd = buffer.toString().trim();
                    cmd = cmd.substring(0, cmd.length() - 1).trim() + ";";
                    byte[] statementBytes = cmd.getBytes("UTF-8");
                    if (cmd.equalsIgnoreCase("quit;")
                            || cmd.equalsIgnoreCase("exit;")) {
                        shutdown();
                        quit = true;
                        break;
                    }

                    long startTime = System.currentTimeMillis();
                    processNextStatement(new ByteArrayInputStream(statementBytes));
                    long time = System.currentTimeMillis() - startTime;
                    System.out.printf("----------------\n%.2f seconds\n\n", ((double) time / 1000.0));

                    // Grab the remainder of the line
                    line = line.substring(split + 1);
                    buffer = new StringBuilder();
                }
                if (line.length() > 0) {
                    buffer.append(line);
                    buffer.append("\n");
                }

                System.out.print("SimpleDB> ");
            }
        }
    }

    private class TupleArrayIterator implements OpIterator {
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        ArrayList<Tuple> tups;
        Iterator<Tuple> it = null;

        public TupleArrayIterator(ArrayList<Tuple> tups) {
            this.tups = tups;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it = tups.iterator();
        }

        /**
         * @return true if the iterator has more items.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator, or null if there are no more
         * tuples.
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException,
                NoSuchElementException {
            return it.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it = tups.iterator();
        }

        /**
         * Returns the TupleDesc associated with this OpIterator.
         */
        @Override
        public TupleDesc getTupleDesc() {
            return tups.get(0).getTupleDesc();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
        }

    }

    public void setTransaction(Transaction t) {
        transaction = t;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public static void main(String argv[]) throws IOException {

        if (argv.length < 1 || argv.length > 4) {
            System.out.println("Invalid number of arguments.\n" + usage);
            System.exit(0);
        }

        Parser p = new Parser();
        p.start(argv);
    }
}

