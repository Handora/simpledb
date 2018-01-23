package simpledb;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockManager {
    private HashMap<TransactionId, HashMap<PageId, Permissions>> transMap;
    private HashMap<PageId, ReentrantReadWriteLock> pageMap;

    public LockManager() {
        transMap = new HashMap<>();
        pageMap = new HashMap<>();
    }

    public void lockRead(TransactionId tid, PageId pid) {
        ReentrantReadWriteLock rl;
        HashMap<PageId, Permissions> mp;
        synchronized(this) {
          do {
                mp = transMap.get(tid);
                if (mp == null) {
                    mp = new HashMap<>();
                    transMap.put(tid, mp);
                } else if (mp.get(pid) != null) {
                    return ;
                }

                rl = pageMap.get(pid);
                if (rl == null) {
                    rl = new ReentrantReadWriteLock();
                    rl.readLock().lock();
                    pageMap.put(pid, rl);
                } else {
                    if (rl.isWriteLocked()) {
                        break;
                    } else {
                        rl.readLock().lock();
                    }
                }

                mp.put(pid, Permissions.READ_ONLY);
                return ;
          } while (false);
      }

      rl.readLock().lock();
      // TODO
      // do this will race?
      mp.put(pid, Permissions.READ_ONLY);
    }

    public void lockWrite (TransactionId tid, PageId pid) {
        ReentrantReadWriteLock rl;
        HashMap<PageId, Permissions> mp;
        synchronized(this) {
          do {
                mp = transMap.get(tid);
                rl = pageMap.get(pid);
                if (mp == null) {
                    mp = new HashMap<>();
                    transMap.put(tid, mp);
                } else if (mp.get(pid) != null) {
                    if (mp.get(pid) == Permissions.READ_ONLY && rl.getReadLockCount() == 1) {
                        // TODO
                        //   how to deal with race condition
                        rl.readLock().unlock();
                        rl.writeLock().lock();
                        mp.put(pid, Permissions.READ_WRITE);
                        return ;
                    } else if (mp.get(pid) == Permissions.READ_WRITE) {
                        return ;
                    } else {
                        break;
                    }
                }


                if (rl == null) {
                    rl = new ReentrantReadWriteLock();
                    rl.writeLock().lock();
                    pageMap.put(pid, rl);
                } else {
                    break;
                }

                mp.put(pid, Permissions.READ_WRITE);
                return ;
          } while (false);
      }

      rl.writeLock().lock();
      // TODO
      // do this will race?
      mp.put(pid, Permissions.READ_WRITE);
    }


    public synchronized void unlock(TransactionId tid, PageId pid) {
        HashMap<PageId, Permissions> mp = transMap.get(tid);
        ReentrantReadWriteLock rl = pageMap.get(pid);
        try {
            if (mp == null || rl == null) {
                throw new Exception("null unlock");
            }

            if (mp.get(pid) == null) {
                throw new Exception("not hold such lock");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Permissions perm = mp.get(pid);
        mp.remove(pid);
        if (perm == Permissions.READ_WRITE) {
            rl.writeLock().unlock();
        } else {
            rl.readLock().unlock();
        }
    }

    public synchronized void unlockAll(TransactionId tid) {
        HashMap<PageId, Permissions> mp = transMap.get(tid);
        for(PageId m: mp.keySet()){
            unlock(tid, m);
        }

        transMap.remove(tid);
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        HashMap<PageId, Permissions> mp = transMap.get(tid);
        if (mp == null)
            return false;
        return mp.get(p) != null;
    }
}
