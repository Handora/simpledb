package simpledb.customtest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Vector;
import simpledb.*;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

/**
 * Just copy the JoinOptimizerTest in test, and modify it according to my custom implementation
 *
 */
public class JoinOptimizerTest extends SimpleDbTestBase {

    /**
     * Given a matrix of tuples from SystemTestUtil.createRandomHeapFile, create
     * an identical HeapFile table
     *
     * @param tuples
     *            Tuples to create a HeapFile from
     * @param columns
     *            Each entry in tuples[] must have
     *            "columns == tuples.get(i).size()"
     * @param colPrefix
     *            String to prefix to the column names (the columns are named
     *            after their column number by default)
     * @return a new HeapFile containing the specified tuples
     * @throws IOException
     *             if a temporary file can't be created to hand to HeapFile to
     *             open and read its data
     */
    public static HeapFile createDuplicateHeapFile(
            ArrayList<ArrayList<Integer>> tuples, int columns, String colPrefix)
            throws IOException {
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.getPageSize(), columns);
        return Utility.openHeapFile(columns, colPrefix, temp);
    }

    ArrayList<ArrayList<Integer>> tuples1;
    HeapFile f1;
    String tableName1;
    int tableId1;
    TableStats stats1;

    ArrayList<ArrayList<Integer>> tuples2;
    HeapFile f2;
    String tableName2;
    int tableId2;
    TableStats stats2;

    /**
     * Set up the test; create some initial tables to work with
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create some sample tables to work with
        this.tuples1 = new ArrayList<ArrayList<Integer>>();
        this.f1 = SystemTestUtil.createRandomHeapFile(10, 1000, 20, null,
                tuples1, "c");

        this.tableName1 = "TA";
        Database.getCatalog().addTable(f1, tableName1);
        this.tableId1 = Database.getCatalog().getTableId(tableName1);
        System.out.println("tableId1: " + tableId1);

        stats1 = new TableStats(tableId1, 19);
        TableStats.setTableStats(tableName1, stats1);

        this.tuples2 = new ArrayList<ArrayList<Integer>>();
        this.f2 = SystemTestUtil.createRandomHeapFile(10, 10000, 20, null,
                tuples2, "c");

        this.tableName2 = "TB";
        Database.getCatalog().addTable(f2, tableName2);
        this.tableId2 = Database.getCatalog().getTableId(tableName2);
        System.out.println("tableId2: " + tableId2);

        stats2 = new TableStats(tableId2, 19);

        TableStats.setTableStats(tableName2, stats2);
    }

    private double[] getRandomJoinCosts(JoinOptimizer jo, LogicalJoinNode js,
            int[] card1s, int[] card2s, double[] cost1s, double[] cost2s) {
        double[] ret = new double[card1s.length];
        for (int i = 0; i < card1s.length; ++i) {
            ret[i] = jo.estimateJoinCost(js, card1s[i], card2s[i], cost1s[i],
                    cost2s[i]);
            // assert that he join cost is no less than the total cost of
            // scanning two tables
            Assert.assertTrue(ret[i] > cost1s[i] + cost2s[i]);
        }
        return ret;
    }

    /**
     * Verify that the join cardinalities produced by estimateJoinCardinality()
     * with equality are implemented according to my implementation
     *
     * Add code to perform more advanced join cardinality estimation.
     * Rather than using simple heuristics to estimate join cardinality,
     * devise a more sophisticated algorithm.
     *
     * way to estimate the cardinality of a join is to assume that
     * each value in the smaller table has a matching value in the larger table.
     * Then the formula for the join selectivity would be: 1/(Max(num-distinct(t1,
     * column1), num-distinct(t2, column2))). Here, column1 and column2 are the
     * join attributes. The cardinality of the join is then the product of the
     * cardinalities of t1 and t2 times the selectivity.
     *
     */
    @Test
    public void estimateJoinCardinality() throws ParsingException {
        TransactionId tid = new TransactionId();
        Parser p = new Parser();
        JoinOptimizer j = new JoinOptimizer(p.generateLogicalPlan(tid,
                "SELECT * FROM " + tableName2 + " t1, " + tableName2
                        + " t2 WHERE t1.c8 = t2.c7;"),
                new Vector<LogicalJoinNode>());

        double cardinality;

        int distinctNum1 = stats1.getDistinctNum(3);
        int distinctNum2 = stats2.getDistinctNum(4);
        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2",
                "c" + Integer.toString(3), "c" + Integer.toString(4),
                Predicate.Op.EQUALS), stats1.estimateTableCardinality(0.8),
                stats2.estimateTableCardinality(0.2), false, false, TableStats
                        .getStatsMap());


        Assert.assertTrue((int)(1.0/(double)((Math.max(distinctNum1, distinctNum2))) * stats1.estimateTableCardinality(0.8) * stats2.estimateTableCardinality(0.2)) == cardinality);

        cardinality = j.estimateJoinCardinality(new LogicalJoinNode("t1", "t2",
                "c" + Integer.toString(3), "c" + Integer.toString(4),
                Predicate.Op.EQUALS), stats1.estimateTableCardinality(0.4),
                stats2.estimateTableCardinality(0.5), false, false, TableStats
                        .getStatsMap());


        Assert.assertTrue((int)(1.0/(double)((Math.max(distinctNum1, distinctNum2))) * stats1.estimateTableCardinality(0.4) * stats2.estimateTableCardinality(0.5)) == cardinality);

    }


    /**
     * Test a much-larger join ordering, to confirm that it executes in a
     * more fast way
     *
     * Improved subset iterator. Our implementation of enumerateSubsets is
     * quite inefficient, because it creates a large number of Java objects
     * on each invocation. A better approach would be to implement an iterator
     * that, for example, returns a BitSet that specifies the elements in the
     * joins vector that should be accessed on each iteration. In this bonus
     * exercise, you would improve the performance of enumerateSubsets so that
     * your system could perform query optimization on plans with 20 or more
     * joins (currently such plans takes minutes or hours to compute).
     *
     ×
     × test with 20 tables to confirm it's correctness
     */
    @Test(timeout = 20000)
    public void fastLargeJoinsTest() throws IOException, DbException,
            TransactionAbortedException, ParsingException {
        final int IO_COST = 103;

        JoinOptimizer j;
        HashMap<String, TableStats> stats = new HashMap<String, TableStats>();
        Vector<LogicalJoinNode> result;
        Vector<LogicalJoinNode> nodes = new Vector<LogicalJoinNode>();
        HashMap<String, Double> filterSelectivities = new HashMap<String, Double>();
        TransactionId tid = new TransactionId();

        // Create a large set of tables, and add tuples to the tables
        ArrayList<ArrayList<Integer>> smallHeapFileTuples = new ArrayList<ArrayList<Integer>>();
        HeapFile smallHeapFileA = SystemTestUtil.createRandomHeapFile(2, 100,
                Integer.MAX_VALUE, null, smallHeapFileTuples, "c");
        HeapFile smallHeapFileB = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileC = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileD = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileE = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileF = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileG = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileH = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileI = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileJ = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileK = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileL = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileM = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileN = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileO = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileP = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileQ = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");
        HeapFile smallHeapFileR = createDuplicateHeapFile(smallHeapFileTuples,
                2, "c");

        ArrayList<ArrayList<Integer>> bigHeapFileTuples = new ArrayList<ArrayList<Integer>>();
        for (int i = 0; i < 100000; i++) {
            bigHeapFileTuples.add(smallHeapFileTuples.get(i % 100));
        }
        HeapFile bigHeapFile = createDuplicateHeapFile(bigHeapFileTuples, 2,
                "c");

        // Add the tables to the database
        Database.getCatalog().addTable(bigHeapFile, "bigTable");
        Database.getCatalog().addTable(smallHeapFileA, "a");
        Database.getCatalog().addTable(smallHeapFileB, "b");
        Database.getCatalog().addTable(smallHeapFileC, "c");
        Database.getCatalog().addTable(smallHeapFileD, "d");
        Database.getCatalog().addTable(smallHeapFileE, "e");
        Database.getCatalog().addTable(smallHeapFileF, "f");
        Database.getCatalog().addTable(smallHeapFileG, "g");
        Database.getCatalog().addTable(smallHeapFileH, "h");
        Database.getCatalog().addTable(smallHeapFileI, "i");
        Database.getCatalog().addTable(smallHeapFileJ, "j");
        Database.getCatalog().addTable(smallHeapFileK, "k");
        Database.getCatalog().addTable(smallHeapFileL, "l");
        Database.getCatalog().addTable(smallHeapFileM, "m");
        Database.getCatalog().addTable(smallHeapFileN, "n");
        Database.getCatalog().addTable(smallHeapFileO, "o");
        Database.getCatalog().addTable(smallHeapFileP, "p");
        Database.getCatalog().addTable(smallHeapFileQ, "q");
        Database.getCatalog().addTable(smallHeapFileR, "r");

        // Come up with join statistics for the tables
        stats.put("bigTable", new TableStats(bigHeapFile.getId(), IO_COST));
        stats.put("a", new TableStats(smallHeapFileA.getId(), IO_COST));
        stats.put("b", new TableStats(smallHeapFileB.getId(), IO_COST));
        stats.put("c", new TableStats(smallHeapFileC.getId(), IO_COST));
        stats.put("d", new TableStats(smallHeapFileD.getId(), IO_COST));
        stats.put("e", new TableStats(smallHeapFileE.getId(), IO_COST));
        stats.put("f", new TableStats(smallHeapFileF.getId(), IO_COST));
        stats.put("g", new TableStats(smallHeapFileG.getId(), IO_COST));
        stats.put("h", new TableStats(smallHeapFileH.getId(), IO_COST));
        stats.put("i", new TableStats(smallHeapFileI.getId(), IO_COST));
        stats.put("j", new TableStats(smallHeapFileJ.getId(), IO_COST));
        stats.put("k", new TableStats(smallHeapFileK.getId(), IO_COST));
        stats.put("l", new TableStats(smallHeapFileL.getId(), IO_COST));
        stats.put("m", new TableStats(smallHeapFileM.getId(), IO_COST));
        stats.put("n", new TableStats(smallHeapFileN.getId(), IO_COST));
        stats.put("o", new TableStats(smallHeapFileO.getId(), IO_COST));
        stats.put("p", new TableStats(smallHeapFileO.getId(), IO_COST));
        stats.put("q", new TableStats(smallHeapFileQ.getId(), IO_COST));
        stats.put("r", new TableStats(smallHeapFileR.getId(), IO_COST));

        // Put in some filter selectivities
        filterSelectivities.put("bigTable", 1.0);
        filterSelectivities.put("a", 1.0);
        filterSelectivities.put("b", 1.0);
        filterSelectivities.put("c", 1.0);
        filterSelectivities.put("d", 1.0);
        filterSelectivities.put("e", 1.0);
        filterSelectivities.put("f", 1.0);
        filterSelectivities.put("g", 1.0);
        filterSelectivities.put("h", 1.0);
        filterSelectivities.put("i", 1.0);
        filterSelectivities.put("j", 1.0);
        filterSelectivities.put("k", 1.0);
        filterSelectivities.put("l", 1.0);
        filterSelectivities.put("m", 1.0);
        filterSelectivities.put("n", 1.0);
        filterSelectivities.put("o", 1.0);
        filterSelectivities.put("p", 1.0);
        filterSelectivities.put("q", 1.0);
        filterSelectivities.put("r", 1.0);

        // Add the nodes to a collection for a query plan
        nodes.add(new LogicalJoinNode("a", "b", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("b", "c", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("c", "d", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("d", "e", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("e", "f", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("f", "g", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("g", "h", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("h", "i", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("i", "j", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("j", "k", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("k", "l", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("l", "m", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("m", "n", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("n", "o", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("o", "p", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("p", "q", "c0", "c0", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("q", "r", "c1", "c1", Predicate.Op.EQUALS));
        nodes.add(new LogicalJoinNode("r", "bigTable", "c0", "c0",
                Predicate.Op.EQUALS));

        // Make sure we don't give the nodes to the optimizer in a nice order
        Collections.shuffle(nodes);
        Parser p = new Parser();
        j = new JoinOptimizer(
                p.generateLogicalPlan(
                        tid,
                        "SELECT COUNT(a.c0) FROM bigTable, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r WHERE bigTable.c0 = r.c0 AND a.c1 = b.c1 AND b.c0 = c.c0 AND c.c1 = d.c1 AND d.c0 = e.c0 AND e.c1 = f.c1 AND f.c0 = g.c0 AND g.c1 = h.c1 AND h.c0 = i.c0 AND i.c1 = j.c1 AND j.c0 = k.c0 AND k.c1 = l.c1 AND l.c0 = m.c0 AND m.c1 = n.c1 AND n.c0 = o.c0 AND o.c1 = p.c1 AND p.c0 = q.c0 AND q.c1 = r.c1;"),
                nodes);

        // Set the last boolean here to 'true' in order to have orderJoins()
        // print out its logic
        result = j.orderJoins(stats, filterSelectivities, false);

        // If you're only re-ordering the join nodes,
        // you shouldn't end up with more than you started with
        Assert.assertEquals(result.size(), nodes.size());

        // Make sure that "bigTable" is the outermost table in the join
        Assert.assertEquals(result.get(result.size() - 1).t2Alias, "bigTable");
    }
}
