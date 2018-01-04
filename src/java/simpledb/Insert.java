package simpledb;

import java.io.*;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private TupleDesc schema;
    private boolean first;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        Type[] ts = new Type[1];
        ts[0] = Type.INT_TYPE;
        this.schema = new TupleDesc(ts);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.schema;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        this.first = true;
    }

    public void close() {
        // some code goes here
        child.close();
        super.close();
        this.first = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!this.first) {
          return null;
        }
        this.first = false;
        int sum = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(this.tid, this.tableId, child.next());
            } catch (IOException e) {
                continue;
            }
            sum ++;
        }

        Tuple result = new Tuple(this.schema);
        result.setField(0, new IntField(sum));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children.length < 1) {
          return ;
        }
        this.child = children[0];
    }
}
