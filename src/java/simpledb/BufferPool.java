package simpledb;

import simpledb.dbfile.DbFile;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.page.Page;
import simpledb.page.pageid.PageId;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;
import simpledb.transaction.TransactionManager;
import simpledb.tuple.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;

    private final Map<PageId, Page> bufferPool = new ConcurrentHashMap<>();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // TODO 参数校验

        /*
            对目标页面尝试落锁，否则阻塞当前线程
         */
        TransactionManager.getInstance().tryToAcquireLockOnThePage(tid, pid, perm);

        Page page = bufferPool.get(pid);
        if (page == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbFile.readPage(pid);
            if (page != null) {
                if (bufferPool.size() >= this.numPages) {
                    this.evictPage();
                }
                if (perm == Permissions.READ_WRITE) {
                    page.markDirty(true, tid);
                }
                bufferPool.put(pid, page);
            }
        }

        /*
            只能返回本事务内的脏页，否则必须返回干净页
         */
        return page;
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return TransactionManager.getInstance().holdsLock(tid, p);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        TransactionManager.getInstance().releasePage(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        this.transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) throws IOException {
        if (commit) {
            flushPages(tid);
        } else {
            for (Map.Entry<PageId, Page> group : bufferPool.entrySet()) {
                Page page = group.getValue();
                if (tid.equals(page.isDirty())) {
                    discardPage(group.getKey());
                }

                this.releasePage(tid, page.getId());
                assert !this.holdsLock(tid, page.getId());
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        ArrayList<Page> dirtyPages = Database.getCatalog().getDatabaseFile(tableId)
                .insertTuple(tid, t);

        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            this.bufferPool.put(dirtyPage.getId(), dirtyPage);
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO 参数校验

        PageId pageId = t.getRecordId().getPageId();
        int tableId = pageId.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        dbFile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     * <p>
     * Notice that BufferPool asks you to implement a flushAllPages() method.
     * This is not something you would ever need in a real implementation of a
     * buffer pool. However, we need this method for testing purposes.
     * You should never call this method from any real code.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> group : bufferPool.entrySet()) {
            Page page = group.getValue();
            if (page.isDirty() != null) {
                this.flushPage(group.getKey());
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * You should also implement discardPage() to
     * remove a page from the buffer pool without flushing it to disk.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        if (pid == null) {
            return;
        }
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        Page pageToBeFlushed = bufferPool.get(pid);
        TransactionId tid = pageToBeFlushed.isDirty();
        if (pageToBeFlushed != null && tid != null) {
            Page before = pageToBeFlushed.getBeforeImage();
            // flushPage本身无事务控制，不应该调用setBeforeImage
            // pageToBeFlushed.setBeforeImage();
            Database.getLogFile().logWrite(tid, before, pageToBeFlushed);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageToBeFlushed);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {

        for (Map.Entry<PageId, Page> group : this.bufferPool.entrySet()) {
            PageId pid = group.getKey();
            Page pageToBeFlushed = group.getValue();
            TransactionId holdOnTid = pageToBeFlushed.isDirty();
            Page before = pageToBeFlushed.getBeforeImage();
            pageToBeFlushed.setBeforeImage();
            if (holdOnTid != null && holdOnTid.equals(tid)) {
                /*
                    此处的逻辑顺序必须严格按照如下代码执行，不可乱序
                    即:setBeforeImage 必须在 getBeforeImage之后，
                    logWrite必须在writePage之前，遵循 WPL
                 */
                Database.getLogFile().logWrite(tid, before, pageToBeFlushed);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageToBeFlushed);


            }
            /*
                落盘后，释放事务在这个页上面的锁
            */
            this.releasePage(tid, pid);

            assert !this.holdsLock(tid, pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {

        /*
            TODO BUG TO BE FIXED
            当前的evict策略有一个问题，当事务持有的写权限页面过多时，
            buffer pool无法直接丢弃这些页面，会抛出异常，这是不正常的。

            考虑TableStatsTest.java 和 systemtest Transaction.java考虑的情况
         */

        for (Map.Entry<PageId, Page> group : bufferPool.entrySet()) {
            Page page = group.getValue();
            if (page.isDirty() == null) {
                bufferPool.remove(group.getKey());
                return;
            }
        }

        throw new DbException("Can't evict pages! All pages are dirty!");
    }
}
