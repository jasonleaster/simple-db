package simpledb.transaction;

import simpledb.Database;
import simpledb.Debug;
import simpledb.Permissions;
import simpledb.exception.TransactionAbortedException;
import simpledb.page.pageid.PageId;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concurrent Transaction Manager for simple-db
 *
 * @Author: Jason Leaster
 * @Date 2018/02/01
 */
public class TransactionManager {

    /**
     * default time out threshold in mills second == 3 s
     */
    private static final int DEFAULT_TIME_OUT_TH = 30 * 1000;

    private static final TransactionManager INSTANCE = new TransactionManager();

    /**
     * 页面共享锁管理器
     */
    private final Map<PageId, Set<TransactionId>> sharedLockManager = new ConcurrentHashMap<>();

    /**
     * 页面排它锁管理器
     */
    private final Map<PageId, TransactionId> exclusiveLockManager = new ConcurrentHashMap<>();

    /**
     * 等待其他锁可能有多个，一对多的关系
     *  Key Tx 等待 ==> Value Txs
     */
    private final Map<TransactionId, Set<TransactionId>> waitForGraph = new ConcurrentHashMap<>();

    public void reset() {
        synchronized (TransactionManager.class){
            INSTANCE.exclusiveLockManager.clear();
            INSTANCE.sharedLockManager.clear();
            INSTANCE.waitForGraph.clear();
        }
    }

    public static TransactionManager getInstance() {
        return INSTANCE;
    }

    public void tryToAcquireLockOnThePage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {

        /*
            尝试获取共享锁
         */
        if (perm == Permissions.READ_ONLY) {
            while (true) {
                synchronized (exclusiveLockManager) {
                    TransactionId exclusiveLock = exclusiveLockManager.get(pid);
                    boolean haveExclusiveLock = exclusiveLock != null;

                    if (haveExclusiveLock && !exclusiveLock.equals(tid)) {
                        /*
                            已有排它锁，并且页面上持有排它锁的事务与
                            当前事务不是同一事务申请共享锁受到阻塞
                         */
                        //showLocksOnPages();
                        waitForGraph.computeIfAbsent(tid, k -> new HashSet<>()).add(exclusiveLock);

                        Thread.yield();// 让出CPU，尝试参与下一次锁的竞争

                        dealWithPotentialDeadlocks(tid);

                        if (isTimeOutTransaction(tid)) {
                            throw new TransactionAbortedException();
                        }
                    } else if (haveExclusiveLock && exclusiveLock.equals(tid)) {
                        /*
                            已有排它锁，并且页面上持有排它锁的事务与
                            当前事务是同一事务，不需要再获取共享锁
                         */
                        break;
                    } else {
                        // 设置共享锁, 使用Set避免重复添加共享锁
                        sharedLockManager.computeIfAbsent(pid, k -> new HashSet<>()).add(tid);
                        waitForGraph.remove(tid);
                        // 加锁成功直接返回
                        Debug.log("S_Lock#"+ tid.getId() + " On Page: " + pid.getPageNumber());
                        break;
                    }
                }
            }
        } else {
            while (true) {
                synchronized (exclusiveLockManager) {

                    TransactionId exclusiveLock = exclusiveLockManager.get(pid);
                    boolean haveExclusiveLock = exclusiveLock != null;

                    if (haveExclusiveLock && !exclusiveLock.equals(tid)) {
                        /*
                            已有排它锁，并且页面上持有排它锁的事务与
                            当前事务不是同一事务申请排他锁受到阻塞
                         */
                        waitForGraph.computeIfAbsent(tid, k -> new HashSet<>()).add(exclusiveLock);

                        dealWithPotentialDeadlocks(tid);
                        if (isTimeOutTransaction(tid)) {
                            throw new TransactionAbortedException();
                        }
                        Thread.yield();
                    } else if (haveExclusiveLock && exclusiveLock.equals(tid)) {
                        /*
                            已有排它锁，并且页面上持有排它锁的事务与
                            当前事务是同一事务，不需要重新申请排他锁
                         */
                        break;
                    } else {
                        /*
                            无排他锁的情况考虑是否存在共享锁
                         */

                        Set<TransactionId> sharedTransactions = sharedLockManager.get(pid);
                        boolean haveSharedLock = sharedTransactions != null && sharedTransactions.size() > 0;
                        /*
                            尝试获取排它锁，但是已有共享锁受到阻塞,
                         */
                        if (haveSharedLock && !sharedTransactions.contains(tid)) {

                            waitForGraph.computeIfAbsent(tid, k -> new HashSet<>()).addAll(sharedTransactions);

                            dealWithPotentialDeadlocks(tid);
                            Thread.yield();
                            continue;
                        } else if (haveSharedLock && sharedTransactions.contains(tid)) {
                            /*
                                危险操作 :(
                                由于共享锁并不能对页面进行写操作，页面是安全的，干净的，
                                故直接剥夺原持有共享锁权限的事务们，对页面执行加排它锁的操作
                             */
                            sharedTransactions.clear();
                        }

                        // 无共享锁, 设置排它锁
                        assert (sharedLockManager.get(pid) == null || sharedLockManager.get(pid).size() == 0);
                        exclusiveLockManager.put(pid, tid);
                        Debug.log("X_Lock#"+ tid.getId() + " On Page: " + pid.getPageNumber());
                        waitForGraph.remove(tid);
                        break;
                    }
                }
            }
        }
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        // 首先检查排它锁
        synchronized (exclusiveLockManager) {
            if (this.exclusiveLockManager.get(p) != null &&
                    this.exclusiveLockManager.get(p).equals(tid)) {
                return true;
            } else {
                // 查看是否有共享锁
                Set<TransactionId> sharedTransactions = this.sharedLockManager.get(p);
                if (sharedTransactions != null && sharedTransactions.contains(tid)) {
                    return true;
                }
                return false;
            }
        }
    }

    public Set<TransactionId> getLocksOnThePage(PageId pid) {
        synchronized (exclusiveLockManager) {
            Set<TransactionId> locks = new HashSet<>();
            TransactionId xLock = this.exclusiveLockManager.get(pid);
            if (xLock != null) {
                locks.add(xLock);
            } else {
                Set<TransactionId> sLocks = this.sharedLockManager.get(pid);
                if (sLocks != null) {
                    locks = sLocks;
                }
            }
            return locks;
        }
    }

    /**
     * 释放事务相关的所有锁
     * @param tid 事务Id
     * @param pid 页面Id
     */
    public void releasePage(TransactionId tid, PageId pid) {
        if (pid == null || tid == null) {
            return;
        }

        synchronized (exclusiveLockManager) {
            if (this.exclusiveLockManager.get(pid) != null &&
                    this.exclusiveLockManager.get(pid).equals(tid)) {
                this.exclusiveLockManager.remove(pid);
            }
            Set<TransactionId> sharedTransactions = this.sharedLockManager.get(pid);
            if (sharedTransactions != null) {
                sharedTransactions.remove(tid);
            }
        }
    }

    /**
     * 通过wait for graph 检测环的方式判断是否死锁
     */
    private boolean isDeadLockTransaction(TransactionId tid) {
        synchronized (exclusiveLockManager) {
            Set<TransactionId> targets = this.waitForGraph.get(tid);

            if (targets == null || targets.size() == 0) {
                return false;
            } else {
                List<TransactionId> waitingList = new ArrayList<>();
                waitingList.add(tid);
                // 类似于广度优先搜索
                while (true) {
                    boolean noChild = true;
                    Set<TransactionId> nextTargets = new HashSet<>();
                    for (TransactionId target : targets) {
                        Set<TransactionId> waitFor = this.waitForGraph.get(target);

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

    private void dealWithPotentialDeadlocks(TransactionId tid) {
         /*
           tid is waiting for the other transaction to release
           the exclusive lock on the page
         */
        if (this.isDeadLockTransaction(tid)) {
            // break the dead locking
            try {
                waitForGraph.remove(tid);
                Database.getBufferPool().transactionComplete(tid, false);
            } catch (IOException e) {
                Debug.log("Abort Dead lock failed!! This shouldn't happen");
            }
        }
    }

    private void abortTransaction(Transaction txToBeAbort) {
        try {
            txToBeAbort.abort();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isTimeOutTransaction(TransactionId tid) {
        if (System.currentTimeMillis() - tid.getStartTimestamp() > DEFAULT_TIME_OUT_TH) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Only for debugging
     */
    private void showLocksOnPages() {
        synchronized (exclusiveLockManager) {
            for (Map.Entry<PageId, TransactionId> group : exclusiveLockManager.entrySet()) {
                PageId pid = group.getKey();
                TransactionId xlock = group.getValue();
                Debug.log("Page#" + pid.getPageNumber() + " has X-Lock: " + xlock.getId());
            }

            for (Map.Entry<PageId, Set<TransactionId>> group : sharedLockManager.entrySet()) {
                PageId pid = group.getKey();
                Set<TransactionId> slocks = group.getValue();
                for (TransactionId slock : slocks) {
                    Debug.log("Page#" + pid.getPageNumber() + " has S-Lock: " + slock.getId());
                }
            }
        }

    }
}
