package simpledb.dbfile;

import simpledb.BufferPool;
import simpledb.Database;
import simpledb.DbException;
import simpledb.page.HeapPage;
import simpledb.page.Page;
import simpledb.page.pageid.HeapPageId;
import simpledb.page.pageid.PageId;
import simpledb.exception.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private String tableName;
    private TupleDesc tupleDesc;
    private int pageNo;
    private Map<PageId, Page> container;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
        this.pageNo = 0;
        this.container = new HashMap<>();

        final int pageSize = BufferPool.getPageSize();
        this.tableName = f.getName();

        /*
         * 不安全，不推荐，重新思考解决方案
         * Comments:
         *  我个人觉得一个对象没有初始化完全的时候就对外暴露是不安全，不正确的做法
         * 但是目前我项目到其他的解决方案
         */
        Database.getCatalog().addTable(this, tableName);

        /*
         * We have to get the tableId from catalog, otherwise we can't guarantee the
         * consistency of tableId is correct.
         */
        final int tableId = Database.getCatalog().getTableId(tableName);

        byte[] pageBuffer = new byte[pageSize];

        try {
            FileInputStream fis = new FileInputStream(f);

            int offset = 0;
            while (fis.read(pageBuffer,offset, pageSize) > 0) {
                HeapPageId heapPageId = new HeapPageId(tableId, pageNo++);
                HeapPage heapPage = new HeapPage(heapPageId, pageBuffer);
                container.put(heapPageId, heapPage);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        return container.get(pid);
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return pageNo;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator();
    }

    private class HeapFileIterator extends AbstractDbFileIterator {

        private int iterIndex = 0;
        private List<Iterator<Tuple>> iterators = new ArrayList<>();

        @Override
        public void open() throws DbException, TransactionAbortedException {
            for (Map.Entry<PageId, Page> entry : container.entrySet()) {
                HeapPage page = (HeapPage) entry.getValue();
                iterators.add(page.iterator());
            }
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (iterators.size() == 0) {
                return null;
            }

            Iterator<Tuple> iterator = iterators.get(iterIndex);
            while (!iterator.hasNext() && iterIndex < iterators.size()) {
                iterIndex++;
            }

            if (iterIndex == iterators.size()) {
                return null;
            } else {
                return iterator.next();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            super.close();
            iterators.clear();
        }
    }
}

