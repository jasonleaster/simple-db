package simpledb;

import simpledb.dbfile.DbFile;
import simpledb.dbfile.DbFileIterator;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.field.Field;
import simpledb.field.IntField;
import simpledb.field.StringField;
import simpledb.histogram.IntHistogram;
import simpledb.histogram.StringHistogram;
import simpledb.operator.Predicate;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a query.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> STATS_MAP = new ConcurrentHashMap<>();

    private final Map<Integer, IntHistogram> intHistograms = new HashMap<>();
    private final Map<Integer, StringHistogram> stringHistograms = new HashMap<>();
    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    private static final int NUM_HIST_BINS = 100;

    private static final int IO_COST_PER_PAGE = 1000;

    private int tableId;
    private int ioCostPerPage;
    private DbFile dbFile;
    private int totalTuples;
    private int estimateScanCost;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableId       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableId, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
        // then scan through its tuples and calculate the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything in a single scan of the table.
        // some code goes here

        this.tableId = tableId;
        this.ioCostPerPage = ioCostPerPage;
        this.dbFile = Database.getCatalog().getDatabaseFile(tableId);
        TupleDesc tupleDesc = this.dbFile.getTupleDesc();

        Map<Integer, Integer> mins = new HashMap<>();
        Map<Integer, Integer> maxs = new HashMap<>();
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                mins.put(i, Integer.MAX_VALUE);
                maxs.put(i, Integer.MIN_VALUE);
            } else {
                stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }
        Transaction tx = new Transaction();
        tx.start();
        DbFileIterator dbFileIterator = dbFile.iterateForReadOnly(tx.getId());
        try {
            int count = 0;
            dbFileIterator.open();
            while (dbFileIterator.hasNext()) {
                final Tuple tuple = dbFileIterator.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i).equals(Type.INT_TYPE)) {
                        IntField field = (IntField) tuple.getField(i);
                        if (field.getValue() < mins.get(i)) {
                            mins.put(i, field.getValue());
                        }
                        if (field.getValue() > maxs.get(i)) {
                            maxs.put(i, field.getValue());
                        }
                    }
                }
                count++;
            }
            this.totalTuples = count;

            for (Integer key : mins.keySet()) {
                if (mins.get(key) <= maxs.get(key)) {
                    intHistograms.put(key, new IntHistogram(NUM_HIST_BINS, mins.get(key), maxs.get(key)));
                } else {
                    Debug.log("This should not happen!!");
                    intHistograms.put(key, new IntHistogram(NUM_HIST_BINS, Integer.MIN_VALUE, Integer.MAX_VALUE));
                }
            }

            dbFileIterator.rewind();

            while (dbFileIterator.hasNext()) {
                final Tuple tuple = dbFileIterator.next();
                for (Integer idx : intHistograms.keySet()) {
                    intHistograms.get(idx).addValue(((IntField) tuple.getField(idx)).getValue());
                }
                for (Integer idx : stringHistograms.keySet()) {
                    stringHistograms.get(idx).addValue(((StringField) tuple.getField(idx)).getValue());
                }
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            dbFileIterator.close();
            try {
                tx.commit();
            } catch (IOException e) {
            }
        }


        final int pageSize = BufferPool.getPageSize();
        int totalPages = (dbFile.getTupleDesc().getSize() * totalTuples + (pageSize - 1)) / pageSize;
        this.estimateScanCost = totalPages * ioCostPerPage;
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return estimateScanCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (this.totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant.getType() == Type.STRING_TYPE) {
            StringHistogram histogram = stringHistograms.get(field);
            if (histogram != null) {
                StringField stringField = (StringField) constant;
                return histogram.estimateSelectivity(op, stringField.getValue());
            }
        } else {
            IntHistogram histogram = intHistograms.get(field);
            if (histogram != null) {
                IntField intField = (IntField) constant;
                return histogram.estimateSelectivity(op, intField.getValue());
            }
        }

        return -1;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        return this.totalTuples;
    }

    public static TableStats getTableStats(String tablename) {
        return STATS_MAP.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        STATS_MAP.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("STATS_MAP");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return STATS_MAP;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableId = tableIt.next();
            TableStats s = new TableStats(tableId, IO_COST_PER_PAGE);
            setTableStats(Database.getCatalog().getTableName(tableId), s);
        }
        System.out.println("Done.");
    }


}
