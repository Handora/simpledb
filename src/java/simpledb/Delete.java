package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private OpIterator child;
    private TupleDesc schema;
    private boolean first;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
              Database.getBufferPool().deleteTuple(this.tid, child.next());
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
