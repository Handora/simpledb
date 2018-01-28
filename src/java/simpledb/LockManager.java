package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collection;
import java.util.concurrent.locks.*;
import java.util.concurrent.TimeUnit;


public class LockManager {
    private Map<TransactionId, Set<PageId>> transMap;
    private Map<PageId, RWLock> pageMap;
    private int DEFAULT_TIMEOUT = 1;

    public class RWLock {
        public int readCount;
        public boolean isWriteLocked;
        public boolean locked;
        public PageId pid;
        public Lock lock;
        public Condition condition;

        public RWLock(PageId pid) {
            this.readCount = 0;
            this.isWriteLocked = false;
            this.locked = false;
            this.pid = pid;
            this.lock = new ReentrantLock();
            this.condition = lock.newCondition();
        }
    }

    public LockManager() {
        transMap = new ConcurrentHashMap<>();
        pageMap = new ConcurrentHashMap<>();
    }

    public Map<PageId, RWLock> getPageLockMap() {
        return this.pageMap;
    }

    public Set<PageId> getTransactionPid(TransactionId tid) {
        return transMap.get(tid);
    }

    public void lockRead(TransactionId tid, PageId pid)
          throws TransactionAbortedException {
        // we don't need to synchronize on tid,
        // because the same tid will not happnen the same time

        // TODO:
        //    CAN WE DO BETTER?
        RWLock l;
        synchronized(this) {
            l = pageMap.get(pid);
            if (l == null) {
                l = new RWLock(pid);
                pageMap.put(pid, l);
            }
        }


        l.lock.lock();
        Set<PageId> trans;
        do {
            synchronized(tid) {
                trans = transMap.get(tid);
                if (trans == null) {
                    trans = Collections.newSetFromMap(new ConcurrentHashMap<PageId, Boolean>());
                    transMap.put(tid, trans);
                }
            }

            if (l.locked == false) {
                l.locked = true;
                l.readCount = 1;
                l.isWriteLocked = false;
                synchronized(tid) {
                    trans.add(l.pid);
                }
                l.lock.unlock();
                return ;
            }



            boolean isOwned = trans.contains(l.pid);
            if (isOwned) {
                l.lock.unlock();
                return ;
            } else {
                if (!l.isWriteLocked) {
                    l.readCount++;
                    synchronized(tid) {
                        trans.add(l.pid);
                    }
                    l.lock.unlock();
                    return ;
                } else {
                    try {
                        boolean ok = l.condition.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                        if (!ok) {
                            l.lock.unlock();
                            throw new TransactionAbortedException();
                        }
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            }
        } while (true);
    }

    public void lockWrite (TransactionId tid, PageId pid)
          throws TransactionAbortedException {
        RWLock l;
        synchronized(this) {
            l = pageMap.get(pid);
            if (l == null) {
                l = new RWLock(pid);
                pageMap.put(pid, l);
            }
        }


        l.lock.lock();
        Set<PageId> trans;
        do {
            synchronized(tid) {
                trans = transMap.get(tid);
                if (trans == null) {
                    trans = Collections.newSetFromMap(new ConcurrentHashMap<PageId, Boolean>());
                    transMap.put(tid, trans);
                }
            }

            if (l.locked == false) {
                l.locked = true;
                l.readCount = 0;
                l.isWriteLocked = true;
                synchronized(tid) {
                    trans.add(l.pid);
                }
                l.lock.unlock();
                return ;
            }

            boolean isOwned = trans.contains(l.pid);
            if (isOwned) {
                if (!l.isWriteLocked) {
                    if (l.readCount == 1) {
                        l.readCount = 0;
                        l.isWriteLocked = true;
                        l.lock.unlock();
                        return ;
                    } else {
                        try {
                            boolean ok = l.condition.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                            if (!ok) {
                                l.lock.unlock();
                                throw new TransactionAbortedException();
                            }
                        } catch(InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    }
                } else {
                    l.lock.unlock();
                    return ;
                }
            } else {
                try {
                    boolean ok = l.condition.await(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
                    if (!ok) {
                        l.lock.unlock();
                        throw new TransactionAbortedException();
                    }
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }
        } while (true);

    }


    public void unlock(TransactionId tid, PageId pid) {
        RWLock l;
        synchronized(this) {
            l = pageMap.get(pid);
            if (l == null) {
                l = new RWLock(pid);
                pageMap.put(pid, l);
            }
        }

        try {
            l.lock.lock();
            Set<PageId> trans;
            trans = transMap.get(tid);
            if (trans == null) {
                throw new DbException("unlock empty tid");
            }

            if (trans.contains(l.pid) == false) {
                throw new DbException("unlock none pid");
            }

            if (l.locked == false) {
                throw new DbException("unlock nonlocked locks");
            }
            synchronized(tid) {
                trans.remove(l.pid);
            }
            if (!l.isWriteLocked) {
                l.readCount --;
                if (l.readCount == 0) {
                    l.locked = false;
                    l.condition.signal();
                }
            } else {
                l.locked = false;
                l.isWriteLocked = false;
                l.condition.signal();
            }
            l.lock.unlock();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public boolean holdsLock(TransactionId tid, PageId pid) {
        synchronized(this) {
            RWLock l = pageMap.get(pid);
            if (l == null) {
                l = new RWLock(pid);
                pageMap.put(pid, l);
                return false;
            }
            return transMap.get(tid) != null && transMap.get(tid).contains(l.pid) != false;
        }
    }

    public void cleanTransaction(TransactionId tid) {
        this.transMap.remove(tid);
    }
}
