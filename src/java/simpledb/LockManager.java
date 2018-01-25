package simpledb;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class LockManager {
    private Map<TransactionId, HashSet<PageId>> transMap;
    private Map<PageId, RWLock> pageMap;

    private class RWLock {
        public int readCount;
        public boolean isWriteLocked;
        public boolean locked;
        public PageId pid;

        public RWLock(PageId pid) {
            this.readCount = 0;
            this.isWriteLocked = false;
            this.locked = false;
            this.pid = pid;
        }
    }

    public LockManager() {
        transMap = new HashMap<>();
        pageMap = new HashMap<>();
    }

    public void lockRead(TransactionId tid, PageId pid) {
        // we don't need to synchronize on tid,
        // because the same tid will not happnen the same time
        synchronized(pid) {
            do {
                RWLock l = pageMap.get(pid);
                if (l == null) {
                    l = new RWLock(pid);
                    pageMap.put(pid, l);
                }

                HashSet<PageId> trans = transMap.get(tid);
                if (trans == null) {
                    trans = new HashSet<>();
                    transMap.put(tid, trans);
                }

                if (l.locked == false) {
                    l.locked = true;
                    l.readCount = 1;
                    l.isWriteLocked = false;
                    trans.add(l.pid);

                    return ;
                }

                boolean isOwned = trans.contains(l.pid);
                if (isOwned) {
                    return ;
                } else {
                    if (!l.isWriteLocked) {
                        l.readCount++;
                        trans.add(l.pid);
                        return ;
                    } else {
                        try {
                            pid.wait();
                        } catch(InterruptedException e) {
                            e.printStackTrace();
                        }
                        continue;
                    }
                }
            } while (true);
        }
    }

    public void lockWrite (TransactionId tid, PageId pid) {
        synchronized(pid) {
            do {
                RWLock l = pageMap.get(pid);
                if (l == null) {
                    l = new RWLock(pid);
                    pageMap.put(pid, l);
                }

                HashSet<PageId> trans = transMap.get(tid);
                if (trans == null) {
                    trans = new HashSet<>();
                    transMap.put(tid, trans);
                }

                if (l.locked == false) {
                    l.locked = true;
                    l.readCount = 0;
                    l.isWriteLocked = true;
                    trans.add(l.pid);
                    return ;
                }

                boolean isOwned = trans.contains(l.pid);
                if (isOwned) {
                    if (!l.isWriteLocked) {
                        if (l.readCount == 1) {
                            l.readCount = 0;
                            l.isWriteLocked = true;
                            return ;
                        } else {
                            try {
                                pid.wait();
                            } catch(InterruptedException e) {
                                e.printStackTrace();
                            }
                            continue;
                        }
                    } else {
                        return ;
                    }
                } else {
                    try {
                        pid.wait();
                    } catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                    continue;
                }
            } while (true);
        }
    }


    public void unlock(TransactionId tid, PageId pid) {
        synchronized(pid) {
            RWLock l = pageMap.get(pid);
            if (l == null) {
                l = new RWLock(pid);
                pageMap.put(pid, l);
            }

            try {
                HashSet<PageId> trans = transMap.get(tid);
                if (trans == null) {
                    throw new DbException("unlock empty tid");
                }

                if (trans.contains(l.pid) == false) {
                    throw new DbException("unlock none pid");
                }

                if (l.locked == false) {
                    throw new DbException("unlock nonlocked locks");
                }

                trans.remove(l.pid);
                if (!l.isWriteLocked) {
                    l.readCount --;
                    if (l.readCount == 0) {
                        l.locked = false;
                        pid.notify();
                    }
                } else {
                    l.locked = false;
                    l.isWriteLocked = false;
                    pid.notify();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public boolean holdsLock(TransactionId tid, PageId pid) {
        synchronized(pid) {
            RWLock l = pageMap.get(pid);
            if (l == null) {
                l = new RWLock(pid);
                pageMap.put(pid, l);
                return false;
            }
            return transMap.get(tid) != null && transMap.get(tid).contains(l.pid) != false;
        }
    }
}
