package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    TransactionId transId;
    int tableId;
    String tableAlias;
    HeapFile.HeapIterator heapFileIterator = null; 
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
	this.transId = tid;
	this.tableId = tableid;
	this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
	// some code goes here
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
	this.tableId = tableid;
	this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
	HeapFile heapFile = (HeapFile)Database.getCatalog().getDatabaseFile(tableId);
	heapFileIterator = (HeapFile.HeapIterator)heapFile.iterator(transId);
	heapFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
	// this is deep copy according to the tupleDesc implementation
	TupleDesc schema = Database.getCatalog().getTupleDesc(tableId);
	TupleDesc another = null;
	String[] names = new String[schema.numFields()];
	Type[] types = new Type[schema.numFields()];
	Iterator<TupleDesc.TDItem> it = schema.iterator();;
	int i=0;
	
	while (it.hasNext()) {
	    TupleDesc.TDItem t = it.next();
	    String name = tableAlias + "." + t.fieldName;
	    Type type = t.fieldType;
	    names[i] = name;
	    types[i] = type;
	    i++;
	}
	another = new TupleDesc(types, names);
	return another;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return heapFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return heapFileIterator.next();
    }

    public void close() {
        // some code goes here
	heapFileIterator.close();
	heapFileIterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
	heapFileIterator.rewind();
    }
}
