package simpledb;

import simpledb.dbfile.DbFile;
import simpledb.exception.DbException;
import simpledb.exception.TransactionAbortedException;
import simpledb.page.Page;
import simpledb.page.pageid.PageId;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;
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

    private final Map<PageId, List<TransactionId>> sharedLockManager = new ConcurrentHashMap<>();
    private final Map<PageId, TransactionId> exclusiveLockManager = new ConcurrentHashMap<>();

    // 等待其他锁可能有多个，一对多的关系
    private final Map<TransactionId, List<TransactionId>> waitForGraph = new ConcurrentHashMap<>();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
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
        // some code goes here
        // TODO 参数校验

        Page page = bufferPool.get(pid);
        if (page == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbFile.readPage(pid);
            if (page != null) {
                if (bufferPool.size() >= this.numPages) {
                    this.evictPage();
                }
                bufferPool.put(pid, page);
            }
        }

        // 设置该锁对应的权限
        if (perm == Permissions.READ_ONLY) {
            while (true) {
                synchronized (exclusiveLockManager) {
                    TransactionId exclusiveLock = exclusiveLockManager.get(pid);
                    boolean haveExclusiveLock = exclusiveLock != null;
                    // 已有排它锁受到阻塞
                    if (haveExclusiveLock && !exclusiveLock.equals(tid)) {
                        this.dealWithDeadLock(pid, tid, perm);

                        Thread.yield();
                    } else {
                        // 设置共享锁
                        List<TransactionId> sharedTransactions = sharedLockManager.computeIfAbsent(pid, k -> new ArrayList<>());
                        // 避免重复添加共享锁
                        if (!sharedTransactions.contains(tid)) {
                            sharedTransactions.add(tid);
                        }
                        break;
                    }
                }
            }
        } else {
            while (true) {
                synchronized (exclusiveLockManager) {
                    List<TransactionId> sharedTransactions = sharedLockManager.get(pid);
                    boolean haveSharedLock = sharedTransactions != null && sharedTransactions.size() > 0;
                    // 已有共享锁受到阻塞
                    if (haveSharedLock && !sharedTransactions.contains(tid)) {
                        this.dealWithDeadLock(pid, tid, perm);

                        Thread.yield();
                    } else {
                        // 考虑锁升级的情况
                        if (haveSharedLock && sharedTransactions.contains(tid)) {
                            sharedTransactions.remove(tid);
                        }
                        TransactionId oldTid = exclusiveLockManager.get(pid);
                        boolean haveExclusiveLock = oldTid != null;
                        if (haveExclusiveLock && !oldTid.equals(tid)) {
                            this.dealWithDeadLock(pid, tid, perm);

                            Thread.yield();
                        } else {
                            // 设置排它锁
                            exclusiveLockManager.put(pid, tid);
                            break;
                        }
                    }
                }
            }
        }
        return page;
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        // 首先检查排它锁
        if (this.exclusiveLockManager.get(p).equals(tid)) {
            return true;
        } else {
            // 查看是否有共享锁
            List<TransactionId> sharedTransactions = this.sharedLockManager.get(p);
            if (sharedTransactions != null && sharedTransactions.contains(tid)) {
                return true;
            }

            return false;
        }
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
        // some code goes here
        // not necessary for lab1|lab2
        if (pid == null || tid == null) {
            return;
        }

        synchronized (exclusiveLockManager) {
            this.exclusiveLockManager.remove(pid);
            List<TransactionId> sharedTransactions = this.sharedLockManager.get(pid);
            if (sharedTransactions != null) {
                sharedTransactions.remove(tid);
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        synchronized (exclusiveLockManager) {

            for (Map.Entry<PageId, TransactionId> group : exclusiveLockManager.entrySet()) {
                if (group.getValue() != null && group.getValue().equals(tid)) {
                    exclusiveLockManager.remove(group.getKey());
                }
            }

            for (Map.Entry<PageId, List<TransactionId>> group : sharedLockManager.entrySet()) {
                if (group.getValue() != null && group.getValue().size() > 0) {
                    List<TransactionId> sharedTids = group.getValue();
                    sharedTids.remove(tid);
                }
            }

            if (commit) {
                flushPages(tid);
            } else {
                for (Map.Entry<PageId, Page> group : bufferPool.entrySet()) {
                    discardPage(group.getKey());
                }
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
        // some code goes here
        // not necessary for lab1
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
        // some code goes here
        // not necessary for lab1
        // TODO 参数校验

        PageId pageId = t.getRecordId().getPageId();
        int tableId = pageId.getTableId();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifiedPages = dbFile.deleteTuple(tid, t);
        for (Page dirtyPage : modifiedPages) {
            dbFile.writePage(dirtyPage);
        }

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
        // some code goes here
        // not necessary for lab1
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
        // some code goes here
        // not necessary for lab1
        Page pageToBeFlushed = bufferPool.get(pid);
        TransactionId tid = pageToBeFlushed.isDirty();
        if (pageToBeFlushed != null && tid != null) {
            Page before = pageToBeFlushed.getBeforeImage();
            // flushPage本身无事务控制，不应该调用setBeforeImage
            //pageToBeFlushed.setBeforeImage();
            Database.getLogFile().logWrite(tid, before, pageToBeFlushed);
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageToBeFlushed);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> group : this.bufferPool.entrySet()) {
            PageId pid = group.getKey();
            Page pageToBeFlushed = group.getValue();
            TransactionId holdOnTid = pageToBeFlushed.isDirty();
            Page before = pageToBeFlushed.getBeforeImage();
            pageToBeFlushed.setBeforeImage();
            if (pageToBeFlushed != null && holdOnTid != null && holdOnTid.equals(tid)) {
                /*
                    此处的逻辑顺序必须严格按照如下代码执行，不可乱序
                    即:setBeforeImage 必须在 getBeforeImage之后，
                    logWrite必须在writePage之前，遵循 WPL
                 */
                Database.getLogFile().logWrite(tid, before, pageToBeFlushed);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pageToBeFlushed);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        for (Map.Entry<PageId, Page> group : bufferPool.entrySet()) {
            Page page = group.getValue();
            if (page.isDirty() != null) {
                try {
                    this.flushPage(group.getKey());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            bufferPool.remove(group.getKey());
            return;
        }
    }

    private void dealWithDeadLock(PageId pid, TransactionId tid, Permissions perm) {

        TransactionId exclusiveLock = exclusiveLockManager.get(pid);
        boolean haveExclusiveLock = exclusiveLock != null;

        List<TransactionId> sharedTransactions = sharedLockManager.get(pid);
        boolean haveSharedLock = sharedTransactions != null && sharedTransactions.size() > 0;

        List<TransactionId> waitingTids = new ArrayList<>();
        if (haveExclusiveLock) {
            waitingTids.add(exclusiveLock);
        }

        if (perm == Permissions.READ_WRITE) {
            if (haveSharedLock) {
                waitingTids.addAll(sharedTransactions);
            }
        }

        /*
         *  wait all these tid to finished
         */
        List<TransactionId> waitsFor = this.waitForGraph.computeIfAbsent(tid, k -> new ArrayList<>());
        for (TransactionId waitingTid : waitingTids) {
            if (!waitsFor.contains(waitingTid)) {
                waitsFor.add(waitingTid);
            }
        }

        /*
           tid is waiting for the other transaction to release
           the exclusive lock on the page
         */
        // dead lock checking
        if (this.isDeadLockTransaction(tid)) {
            // break the dead locking
            try {
                this.transactionComplete(tid, false);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Abort Dead lock failed!! This shouldn't happen");
            }
        }
    }

    /**
     * Check if there have dead lock in the transaction waiting graph.
     *
     * @param tid TransactionId
     * @return return true if there have dead lock
     */
    private boolean isDeadLockTransaction(TransactionId tid) {
        List<TransactionId> targets = this.waitForGraph.get(tid);

        if (targets == null || targets.size() == 0) {
            return false;
        } else {
            List<TransactionId> waitingList = new ArrayList<>();
            waitingList.add(tid);
            // 类似于广度优先搜索
            while (true) {
                boolean noChild = true;
                List<TransactionId> nextTargets = new ArrayList<>();
                for (TransactionId target : targets) {
                    List<TransactionId> waitFor = this.waitForGraph.get(target);

                    if (waitFor != null && waitFor.size() > 0) {
                        if (waitingList.contains(target)) {
                            return true;
                        } else {
                            noChild = false;
                            nextTargets.addAll(waitFor);
                            waitingList.addAll(waitFor);
                        }
                    }
                }
                if (noChild) {
                    return false;
                } else {
                    targets = nextTargets;
                }
            }
        }
    }

}
