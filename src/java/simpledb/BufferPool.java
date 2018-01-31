package simpledb;

import java.io.*;
import java.util.*;
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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /** Maximum number of pages in this buffer pool */
    private static int numPages;

    public LockManager manager;

    /** Page storage */
    private class PageBuffer {
        private Page p;
        private PageBuffer next;
        private PageBuffer prev;
        private boolean isSen;

        public PageBuffer(Page p, PageBuffer next, PageBuffer prev, boolean isSen) {
            this.p = p;
            this.next = next;
            this.prev = prev;
            this.isSen = isSen;
        }

        public PageBuffer(Page p, PageBuffer next, PageBuffer prev) {
            this.p = p;
            this.next = next;
            this.prev = prev;
            this.isSen = false;
        }

        public boolean isSential() {
            return this.isSen;
        }

        public PageBuffer(Page p) {
            this(p, null, null);
        }

        public PageBuffer(PageBuffer next, PageBuffer prev) {
            this(null, next, prev);
        }

        public PageBuffer() {
            this(null, null, null);
        }

        public Page getPage() {
            return this.p;
        }

        public void setPage(Page p) {
            this.p = p;
        }

        public PageBuffer getNext() {
            return this.next;
        }

        public void setNext(PageBuffer n) {
            this.next = n;
        }

        public void setPrev(PageBuffer p) {
            this.prev = p;
        }

        public PageBuffer getPrev() {
            return this.prev;
        }
    }

    private class BufferChain {
        private PageBuffer head;
        private HashMap<PageId, PageBuffer> pageMap;

        public BufferChain() {
            head = new PageBuffer(null, null, null, true);
            head.setNext(head);
            head.setPrev(head);
            pageMap = new HashMap<>();
        }

        public void insertFirst(PageBuffer pb) {
            this.head.getNext().setPrev(pb);
            pb.setNext(this.head.getNext());
            pb.setPrev(this.head);
            this.head.setNext(pb);

            pageMap.put(pb.p.getId(), pb);
        }

        public void insertWithoutSetIntoMap(PageBuffer pb) {
            this.head.getNext().setPrev(pb);
            pb.setNext(this.head.getNext());
            pb.setPrev(this.head);
            this.head.setNext(pb);
        }

        public void delete(PageBuffer pb) throws DbException {
            if (this.head.getPrev() == this.head) {
                throw new DbException("delete page from empty bufferpool");
            }

            pb.getPrev().setNext(pb.getNext());
            pb.getNext().setPrev(pb.getPrev());

            pageMap.remove(pb.p.getId());
        }

        public PageBuffer find(PageId pid) {
            PageBuffer pb = pageMap.get(pid);
            return pb;
        }

        public PageBuffer deleteLast() throws DbException {
            if (this.head.getPrev() == this.head) {
                throw new DbException("delete page from empty bufferpool");
            }

            PageBuffer tmp = this.head.getPrev();
            this.head.setPrev(tmp.getPrev());
            tmp.getPrev().setNext(this.head);

            pageMap.remove(tmp.p.getId());
            return tmp;
        }

        public PageBuffer deleteLastWithoutSetOutofMap() throws DbException {
            if (this.head.getPrev() == this.head) {
                throw new DbException("delete page from empty bufferpool");
            }

            PageBuffer tmp = this.head.getPrev();
            this.head.setPrev(tmp.getPrev());
            tmp.getPrev().setNext(this.head);

            return tmp;
        }

        public boolean isEmpty() {
            return this.head.getNext() == this.head;
        }

        public ArrayList<Page> getBufferPages() {
            ArrayList<Page> a = new ArrayList<>();
            for (PageBuffer it = this.head.getNext(); it != this.head; it = it.getNext()) {
                a.add(it.getPage());
            }
            return a;
        }
    }

    private BufferChain buffer, empty;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
      	BufferPool.numPages = numPages;
        buffer = new BufferChain();
        empty = new BufferChain();
        manager = new LockManager();
        for (int i=0; i<numPages; i++) {
            PageBuffer pb = new PageBuffer();
            empty.insertWithoutSetIntoMap(pb);
        }
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
      	if (pid == null)
      	    throw new DbException("null pageId");

        if (perm == Permissions.READ_ONLY)
            manager.lockRead(tid, pid);
        else
            manager.lockWrite(tid, pid);

        synchronized(this) {
            PageBuffer pb = this.buffer.find(pid);
            if (pb != null) {
                buffer.delete(pb);
                buffer.insertFirst(pb);
                return pb.getPage();
            } else {
                if (empty.isEmpty()) {
                    evictPage();
                }

                pb = empty.deleteLastWithoutSetOutofMap();
                DbFile hf = Database.getCatalog().getDatabaseFile(pid.getTableId());
                pb.setPage(hf.readPage(pid));
                buffer.insertFirst(pb);
                return pb.getPage();
            }
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        manager.unlock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return manager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        // TODO:
        //   how should we deal with rangelock or inserted file in HeapFile.java
        Set<PageId> pids = manager.getTransactionPid(tid);
        if (pids == null) {
            return ;
        }
        synchronized(tid) {
            for (PageId pid: pids) {
                manager.unlock(tid, pid);
                if (commit) {
                    this.flushPage(pid);
                } else {
                    this.discardPage(pid);
                }
            }

            manager.cleanTransaction(tid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> ar = hf.insertTuple(tid, t);
        for (Page p: ar) {
            synchronized(this) {
                p.markDirty(true, tid);
                PageBuffer pb = buffer.find(p.getId());

                if (pb != null) {
                    pb.setPage(p);
                    buffer.delete(pb);
                    buffer.insertFirst(pb);
                } else {
                    if (empty.isEmpty()) {
                        evictPage();
                    }

                    pb = empty.deleteLastWithoutSetOutofMap();
                    pb.setPage(p);
                    buffer.insertFirst(pb);
                }
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile hf = (HeapFile)Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> ar = hf.deleteTuple(tid, t);
        for (Page p: ar) {
            synchronized(this) {
                p.markDirty(true, tid);
                PageBuffer pb = buffer.find(p.getId());

                if (pb != null) {
                    pb.setPage(p);
                    buffer.delete(pb);
                    buffer.insertFirst(pb);
                } else {
                    if (empty.isEmpty()) {
                        evictPage();
                    }

                    pb = empty.deleteLastWithoutSetOutofMap();
                    pb.setPage(p);
                    buffer.insertFirst(pb);
                }
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> a = buffer.getBufferPages();
        for (Page v: a) {
            if (v.isDirty() != null) {
                HeapFile h = (HeapFile)Database.getCatalog().getDatabaseFile(v.getId().getTableId());
                h.writePage(v);
                v.markDirty(false, null);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        PageBuffer pb = buffer.find(pid);
        if (pb == null) {
            return ;
        } else {
            try {
                buffer.delete(pb);
            } catch(DbException e) {
                e.printStackTrace();
            }
            empty.insertWithoutSetIntoMap(pb);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        PageBuffer pb = buffer.find(pid);
        if (pb == null) {
            return ;
        } else {
            if (pb.getPage().isDirty() != null) {
                HeapFile h = (HeapFile)Database.getCatalog().getDatabaseFile(pid.getTableId());
                h.writePage(pb.getPage());
                pb.getPage().markDirty(false, null);
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int tryTime = numPages;
        while (tryTime > 0) {
            tryTime --;
            PageBuffer pb = buffer.deleteLast();

            if (pb.getPage().isDirty() != null) {
                // HeapFile h = (HeapFile)Database.getCatalog().getDatabaseFile(pb.getPage().getId().getTableId());
                // try {
                //     h.writePage(pb.getPage());
                // } catch(IOException e) {
                //     e.printStackTrace();
                // }
                // pb.getPage().markDirty(false, null);

                buffer.insertFirst(pb);
                continue;
            }
            empty.insertWithoutSetIntoMap(pb);
            return;
        }

        throw new DbException("No enough place for NO STEAL");
    }
}
