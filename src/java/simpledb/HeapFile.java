package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc schema;

    public class HeapIterator extends AbstractDbFileIterator {
        int pid;
        TransactionId tid;
        LinkedList<Tuple> tuples;

        public HeapIterator(TransactionId t_id) {
      			tuples = null;
      			tid = t_id;
        }

        public void open()
            throws DbException, TransactionAbortedException {
            tuples = new LinkedList<>();
            pid = 0;
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
            if (p == null) {
                return;
            }
            Iterator<Tuple> it = p.iterator();
            while (it.hasNext()) {
                tuples.add(it.next());
            }
            pid++;
        }

        @Override
        protected Tuple readNext()
            throws DbException, TransactionAbortedException {
            // prevent iterator not opened
            if (tuples == null) {
                return null;
            }
            while (tuples.size() == 0) {
                if (pid >= numPages()) {
                    return null;
                } else {
                    HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pid), Permissions.READ_ONLY);
                    if (p == null) {
                        return null;
                    }
                    Iterator<Tuple> it = p.iterator();
                    while (it.hasNext()) {
                        tuples.add(it.next());
                    }
                    pid++;
                }
            }

            return tuples.removeFirst();
        }

        public void rewind()
            throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            super.close();
            tuples = null;
            pid = 0;
        }
    }

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.schema = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
    	  return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	  return schema;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
      	if (pid.getTableId() != getId() || pid.getPageNumber() >= numPages())
      	    return null;

      	int pgNo = pid.getPageNumber();
      	int offset = pgNo * BufferPool.getPageSize();
      	try {
      	    RandomAccessFile raf = new RandomAccessFile(file, "r");
      	    byte[] b = new byte[BufferPool.getPageSize()];
      	    int readCnt = BufferPool.getPageSize();
      	    raf.seek(offset);
      	    raf.read(b, 0, readCnt);
            raf.close();
      	    return new HeapPage((HeapPageId)pid, b);
      	} catch(Exception e) {
      	    System.out.println("readPage "+e);
      	    return null;
      	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // not necessary for lab1
        int pageNo = page.getId().getPageNumber();
        int offset = pageNo * BufferPool.getPageSize();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.seek(offset);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int)this.file.length() / BufferPool.getPageSize()
                + ((int)this.file.length() % BufferPool.getPageSize() == 0 ? 0:1);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // not necessary for lab1
        int len = numPages();
        int i;
        for (i=0; i<len; i++) {
            HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (p == null) {
                throw new DbException("Internal error");
            }

            if (p.getNumEmptySlots() > 0) {
                p.insertTuple(t);

                ArrayList<Page> a = new ArrayList<>();
                a.add(p);
                return a;
            }
            Database.getBufferPool().manager.unlock(tid, new HeapPageId(getId(), i));
        }

        HeapPage p = new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData());
        ArrayList<Page> a = new ArrayList<>();
        this.writePage(p);
        p.insertTuple(t);
        a.add(p);
        return a;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // not necessary for lab1
        HeapPage p = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        if (p == null)
          return null;
        p.deleteTuple(t);
        ArrayList<Page> a = new ArrayList<>();
        a.add(p);
        return a;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapIterator(tid);
    }

}
