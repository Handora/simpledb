package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 *
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private int tableId;
    private int ioCostPerPage;
    private DbFile file;
    private int[] max;
    private int[] min;
    private int nTuples;
    private TupleDesc tupleDesc;
    private int numFields;
    private boolean[] isInteger;
    private Map<Integer, Object> histograms;
    private int[] distinctFields;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;
        this.file = Database.getCatalog().getDatabaseFile(tableid);
        this.tupleDesc = this.file.getTupleDesc();
        this.numFields = tupleDesc.numFields();
        this.max = new int[numFields];
        this.min = new int[numFields];
        this.isInteger = new boolean[numFields];
        this.histograms = new HashMap<>();
        this.distinctFields = new int[numFields];
        HashSet<Object>[] fieldSet = new HashSet[numFields];

        // TODO
        // How to deal with transaction id
        this.nTuples = 0;
        Transaction txn = new Transaction();
        txn.start();
        DbFileIterator it = this.file.iterator(txn.getId());
        try {
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                Iterator<Field> it2 = t.fields();
                int i = 0;
                while (it2.hasNext()) {
                    Field f = it2.next();
                    if (fieldSet[i] == null) {
                        fieldSet[i] = new HashSet<Object>();
                    }

                    if (!f.getType().equals(Type.INT_TYPE)) {
                        fieldSet[i].add(((StringField)f).getValue());
                        this.isInteger[i] = false;
                        i++;
                        continue;
                    }
                    fieldSet[i].add(((IntField)f).getValue());
                    this.isInteger[i] = true;
                    if (this.nTuples == 0) {
                        this.max[i] = this.min[i] = ((IntField)f).getValue();
                    } else {
                        int v = ((IntField)f).getValue();
                        if (v > max[i]) {
                            max[i] = v;
                        }
                        if (v < min[i]) {
                            min[i] = v;
                        }
                    }
                    i++;
                }
                this.nTuples++;
            }
            it.close();
            for (int i=0; i<numFields; i++) {
                this.distinctFields[i] = fieldSet[i].size();
                fieldSet[i] = null;
            }
            fieldSet = null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            it.open();
            while (it.hasNext()) {
                Tuple t = it.next();
                int i = 0;
                while (i<this.numFields) {
                    if (this.isInteger[i]) {
                        IntHistogram ih = (IntHistogram)this.histograms.get(i);
                        if (ih == null) {
                            ih = new IntHistogram(NUM_HIST_BINS, this.min[i], this.max[i]);
                            this.histograms.put(i, ih);
                        }
                        IntField intf = (IntField)t.getField(i);
                        ih.addValue(intf.getValue());
                    } else {
                        StringHistogram sh = (StringHistogram)this.histograms.get(i);
                        if (sh == null) {
                            sh = new StringHistogram(NUM_HIST_BINS);
                            this.histograms.put(i, sh);
                        }
                        StringField sf = (StringField)t.getField(i);
                        sh.addValue(sf.getValue());
                    }
                    i++;
                }
            }
            it.close();

            txn.transactionComplete(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getDistinctNum(int i) {
        return this.distinctFields[i];
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     *
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        HeapFile hp = (HeapFile)this.file;
        return hp.numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)((double)this.nTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        try {
          if (field < 0 || field >= numFields) {
            throw new DbException("wrong field number");
          }
        } catch(Exception e) {
            e.printStackTrace();
        }

        if (this.isInteger[field]) {
            IntHistogram h = (IntHistogram)this.histograms.get(field);
            return h.estimateSelectivity(op, ((IntField)constant).getValue());
        } else {
            StringHistogram h = (StringHistogram)this.histograms.get(field);
            return h.estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return this.nTuples;
    }

}
