package simpledb.transaction;

import simpledb.Debug;
import simpledb.Permissions;
import simpledb.exception.TransactionAbortedException;
import simpledb.page.pageid.PageId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Concurrent Transaction Manager for simple-db
 * (demo)
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
    private final Map<PageId, List<TransactionId>> sharedLockManager = new ConcurrentHashMap<>();

    /**
     * 页面排它锁管理器
     */
    private final Map<PageId, TransactionId> exclusiveLockManager = new ConcurrentHashMap<>();

    /**
     * 等待其他锁可能有多个，一对多的关系
     */
    private final Map<TransactionId, List<TransactionId>> waitForGraph = new ConcurrentHashMap<>();

    public static TransactionManager getInstance() {
        return INSTANCE;
    }

    public void tryToAcquireLockOnThePage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        if (perm == Permissions.READ_ONLY) {
            while (true) {

                synchronized (exclusiveLockManager) {
                    TransactionId exclusiveLock = exclusiveLockManager.get(pid);
                    boolean haveExclusiveLock = exclusiveLock != null;
                    // 已有排它锁 申请共享锁受到阻塞
                    if (haveExclusiveLock && !exclusiveLock.equals(tid)) {
                        showLocksOnPages();
                        Thread.yield();// 让出CPU，尝试参与下一次锁的竞争

                        if (isTimeOutTransaction(tid)) {
                            throw new TransactionAbortedException();
                        }
                    } else if (haveExclusiveLock && exclusiveLock.equals(tid)) {
                        // 事务已经拥有排它锁，不需要再获取共享锁
                        break;
                    } else {
                        // 设置共享锁
                        List<TransactionId> sharedTransactions = sharedLockManager.computeIfAbsent(pid, k -> new ArrayList<>());
                        // 避免重复添加共享锁
                        if (!sharedTransactions.contains(tid)) {
                            sharedTransactions.add(tid);
                        }
                        // 加锁成功
                        break;
                    }
                }
            }
        } else {
            while (true) {
                synchronized (exclusiveLockManager) {
                    List<TransactionId> sharedTransactions = sharedLockManager.get(pid);
                    boolean haveSharedLock = sharedTransactions != null && sharedTransactions.size() > 0;
                    // 尝试获取排它锁，但是已有共享锁受到阻塞
                    if (haveSharedLock && !sharedTransactions.contains(tid)) {
                        if (isTimeOutTransaction(tid)) {
                            for (int i = 0; i < sharedTransactions.size(); i++ ) {
                                TransactionId sharedTid = sharedTransactions.get(i);
                                try {
                                    assert sharedTid.getId() != tid.getId();
                                    sharedTid.getTransaction().abort();
                                    Debug.log("Rollback tid#" + sharedTid.getId() + " for tid#" + tid.getId());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }

                            // 共享锁被全部释放掉了
                            assert (sharedLockManager.get(pid) == null || sharedLockManager.get(pid).size() == 0) ;

                            // 设置排它锁
                            exclusiveLockManager.put(pid, tid);
                            // 加锁成功
                            break;
                        } else {
                            Thread.yield();
                        }
                    } else if (haveSharedLock && sharedTransactions.contains(tid)) {
                        // 考虑锁升级的情况
                        sharedTransactions.remove(tid);

                        for (int i = 0; i < sharedTransactions.size(); i++ ) {
                            TransactionId sharedTid = sharedTransactions.get(i);
                            try {
                                assert sharedTid.getId() != tid.getId();
                                sharedTid.getTransaction().abort();
                                Debug.log("Rollback tid#" + sharedTid.getId() + " for tid#" + tid.getId());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }

                        // 共享锁被全部释放掉了
                        assert (!(sharedLockManager.get(pid) == null || sharedLockManager.get(pid).size() == 0));

                        exclusiveLockManager.put(pid, tid);
                        // 加锁成功
                        break;

                    } else {
                        // 无共享锁
                        TransactionId oldTid = exclusiveLockManager.get(pid);
                        boolean haveExclusiveLock = oldTid != null;
                        if (haveExclusiveLock && !oldTid.equals(tid)) {
                            if (isTimeOutTransaction(tid.getTransaction())) {
                                throw new TransactionAbortedException();
                            }
                            Thread.yield();
                        } else {
                            // 设置排它锁
                            assert (sharedLockManager.get(pid) == null || sharedLockManager.get(pid).size() == 0);
                            exclusiveLockManager.put(pid, tid);
                            // 加锁成功
                            break;
                        }
                    }
                }
            }
        }
    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        // 首先检查排它锁
        synchronized (exclusiveLockManager) {
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
    }

    public List<TransactionId> getLocksOnThePage(PageId pid) {
        synchronized (exclusiveLockManager) {
            List<TransactionId> locks = new ArrayList<>();
            TransactionId xLock = this.exclusiveLockManager.get(pid);
            if (xLock != null) {
                locks.add(xLock);
            } else {
                List<TransactionId> sLocks = this.sharedLockManager.get(pid);
                if (sLocks != null) {
                    locks = sLocks;
                }
            }

            return locks;
        }
    }

    public void releasePage(TransactionId tid, PageId pid) {
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
     * 通过wait for graph 检测环的方式判断是否死锁
     */
    private boolean isDeadLockTransaction(TransactionId tid) {
        synchronized (exclusiveLockManager) {
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

            for (Map.Entry<PageId, List<TransactionId>> group : sharedLockManager.entrySet()) {
                PageId pid = group.getKey();
                List<TransactionId> slocks = group.getValue();
                for (TransactionId slock : slocks) {
                    Debug.log("Page#" + pid.getPageNumber() + " has S-Lock: " + slock.getId());
                }
            }
        }

    }
}
