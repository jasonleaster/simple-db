package simpledb;

import org.junit.Test;
import simpledb.dbfile.HeapFile;
import simpledb.exception.ParsingException;
import simpledb.field.IntField;
import simpledb.logical.LogicalPlan;
import simpledb.operator.OpIterator;
import simpledb.operator.Predicate;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import javax.xml.crypto.Data;
import java.io.File;
import java.util.HashMap;

public class LogicalPlanTest {

    @Test
    public void test() {
        final String tableName = "some_data_file1";
        final String fileName = tableName + ".dat";

        /*
            construct a 3-column table schema
         */
        final Type[] types   = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        final String[] names = new String[]{"field0", "field1", "field2"};
        final TupleDesc td   = new TupleDesc(types, names);

        /*
            create the tables, associate them with the data files
            and tell the catalog about the schema  the tables.
         */
        HeapFile heapFile = new HeapFile(new File(fileName), td);

        Database.getCatalog().addTable(heapFile, "t1");
        Tuple tupleA = new Tuple(td);
        tupleA.setField(0, new IntField(1));

        Tuple tupleB = new Tuple(td);
        tupleB.setField(0, new IntField(-1));

        Transaction prepareTx = new Transaction();
        prepareTx.start();
        TransactionId tidPreparing = prepareTx.getId();

        LogicalPlan logicalPlan = new LogicalPlan();
        try {
            logicalPlan.addScan(heapFile.getId(), "t1");
            logicalPlan.addProjectField("field0", "");

            Database.getBufferPool().insertTuple(tidPreparing, heapFile.getId(), tupleA);
            Database.getBufferPool().insertTuple(tidPreparing, heapFile.getId(), tupleB);
            prepareTx.commit();

            logicalPlan.addFilter("t1.field0", Predicate.Op.GREATER_THAN, "0");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        OpIterator opIterator;
        try {
            /*
                将逻辑计划转化成物理执行计划，实际为具体的迭代器，
                并且迭代器会遍历执行这个物理执行计划。
             */
            final HashMap<String, TableStats> tableMap = new HashMap<>();
            tableMap.put("t1", new TableStats(heapFile.getId(), 1));

            Transaction tx = new Transaction();
            tx.start();
            TransactionId tid = tx.getId();
            opIterator = logicalPlan.physicalPlan(tid, tableMap, false);
            try {
                opIterator.open();
                int i = 0;
                while (opIterator.hasNext()) {
                    // TODO bug 这里应该只打印一次才对
                    Tuple tup = opIterator.next();
                    Debug.log("times: " + (i++) +"  "+ tup.toString());
                }
                opIterator.close();
                tx.commit();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (ParsingException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

      /*
            Another Test case
            SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
            SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

            // create a filter for the where condition
            Filter sf1 = new Filter( new Predicate(0,
                                    Predicate.AggregateFunc.GREATER_THAN, new IntField(1)),  ss1);

            JoinPredicate predicteOper = new JoinPredicate(1, Predicate.AggregateFunc.EQUALS, 1);
            Join j = new Join(predicteOper, sf1, ss2);
        */
}