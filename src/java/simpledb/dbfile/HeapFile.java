package simpledb.dbfile;


import simpledb.BufferPool;
import simpledb.Database;
import simpledb.Debug;
import simpledb.exception.DbException;
import simpledb.Permissions;
import simpledb.transaction.TransactionId;
import simpledb.exception.TransactionAbortedException;
import simpledb.page.HeapPage;
import simpledb.page.Page;
import simpledb.page.pageid.HeapPageId;
import simpledb.page.pageid.PageId;
import simpledb.tuple.Tuple;
import simpledb.tuple.TupleDesc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the diskFile is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final int tableId;
    private final TupleDesc tupleDesc;
    private final File diskFile;


    /**
     * Constructs a heap diskFile backed by the specified diskFile.
     *
     * @param diskFile the diskFile that stores the on-disk backing
     *                 store for this heap diskFile.
     */
    public HeapFile(File diskFile, TupleDesc td) {
        this.tupleDesc = td;
        this.diskFile  = diskFile;
        this.tableId   = diskFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return diskFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute diskFile name of the
     * diskFile underlying the heapfile, i.e. file.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    @Override
    public Page readPage(PageId pageId) {
        final int pageSize = BufferPool.getPageSize();
        final int offset = pageId.getPageNumber() * pageSize;
        final byte[] pageBuffer = new byte[pageSize];

        HeapPage heapPage = null;
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(diskFile);
            if (offset > 0) {
                fis.skip(offset);
            }

            if (fis.available() > 0) {
                if (fis.read(pageBuffer, 0, pageSize) <= 0) {
                    Debug.log("Failed to read page:" + pageId.getPageNumber());
                } else {
                    heapPage = new HeapPage((HeapPageId) pageId, pageBuffer);
                }
            }
        } catch (IOException e) {
            Debug.log("HeapFile##readPage: " +
                    "Exception happened when database trying to get page from disk");
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Failed to close this file stream");
                }
            }
        }

        return heapPage;
    }

    @Override
    public void writePage(Page page) throws IOException {
        PageId pageId = page.getId();
        int pageNo = pageId.getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        byte[] pageData = page.getPageData();

        RandomAccessFile raf = new RandomAccessFile(this.diskFile, "rw");
        raf.seek(offset);
        raf.write(pageData);
        raf.close();

        page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // 这里的接口建议改成long类型
        long currentPageNo = (diskFile.length() - 1 + BufferPool.getPageSize())
                / BufferPool.getPageSize();
        return (int) currentPageNo;
    }

    /**
     *  TODO 我不是很明白为什么这里返回的页面是个list，而不是单个page
     */
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple tuple)
            throws DbException, IOException, TransactionAbortedException {
         /*
         * Note that it is important that the HeapFile.insertTuple() and
         * HeapFile.deleteTuple() methods access pages using the BufferPool.getPage() method
         */
        ArrayList<Page> pagesModified = new ArrayList<>();
        for (int i = 0; i < this.numPages(); i++) {
             PageId pageId = new HeapPageId(this.tableId, i);
             HeapPage page = (HeapPage) Database.getBufferPool()
                     .getPage(tid, pageId, Permissions.READ_ONLY);

             if (page.getNumEmptySlots() > 0) {
                 page = (HeapPage) Database.getBufferPool()
                         .getPage(tid, pageId, Permissions.READ_WRITE);
                 page.markDirty(true, tid);
                 page.insertTuple(tuple);
                 pagesModified.add(page);
                 return  pagesModified;
             }
         }

         // No more space in existed pages
        HeapPageId pageId = new HeapPageId(this.tableId, this.numPages());
        byte[] emptyPage = new byte[BufferPool.getPageSize()];
        HeapPage newPage = new HeapPage(pageId, emptyPage);
        newPage.markDirty(true, tid);
        newPage.insertTuple(tuple);
        this.writePage(newPage);

        pagesModified.add(newPage);

        return pagesModified;
    }

    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pagesModified = new ArrayList<>();
        for (int i = 0; i < this.numPages(); i++) {
            PageId pageId = new HeapPageId(this.tableId, i);
            HeapPage page = (HeapPage) Database.getBufferPool()
                    .getPage(tid, pageId, Permissions.READ_WRITE);

            if (page == null){
                continue;
            }

            boolean deleteFailed = false;
            try {
                page.deleteTuple(t);
            } catch (DbException e) {
                deleteFailed = true;
            }
            if (!deleteFailed) {
                page.markDirty(true, tid);
                pagesModified.add(page);
            }
        }
        return pagesModified;
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this.numPages(), tid, Permissions.READ_WRITE);
    }

    @Override
    public DbFileIterator iterateForReadOnly(TransactionId tid) {
        return new HeapFileIterator(this.numPages(), tid, Permissions.READ_ONLY);
    }

    private class HeapFileIterator extends AbstractDbFileIterator {

        private TransactionId tid;
        private Permissions permissions;

        HeapFileIterator(int pageNo, TransactionId tid, Permissions permissions) {
            this.pageNo = pageNo;
            this.tid = tid;
            this.permissions = permissions;
        }

        private int pageNo;
        private int iterIndex = 0;
        private Iterator<Tuple> iterator;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterIndex = 0;
            iterator = this.getHeapPageIterator(iterIndex);
            if (iterator == null) {
                throw new DbException("iterator is null");
            }
        }

        private Iterator<Tuple> getHeapPageIterator(int pageNo)
                throws DbException, TransactionAbortedException {
            PageId pageId = new HeapPageId(tableId, pageNo);
            /*
                这里获取页面的权限值得商讨
                1. 如果使用READ_ONLY, 会导致并发事务的系统单元测试挂掉，无解
                2. 使用READ_WRITE，抢占该页数据，避免并发写覆盖的情况，为了通过单元测试。
             */
            Page page = Database.getBufferPool().getPage(tid, pageId, permissions);
            return ((HeapPage) page).iterator();
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (iterator == null || iterIndex >= pageNo) {
                return null;
            }

            while (!iterator.hasNext()) {
                iterIndex++;
                if (iterIndex < pageNo) {
                    iterator = this.getHeapPageIterator(iterIndex);
                } else {
                    break;
                }
            }

            if (iterIndex == pageNo) {
                return null;
            } else {
                return iterator.next();
            }
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            this.iterIndex = pageNo;
        }
    }
}

