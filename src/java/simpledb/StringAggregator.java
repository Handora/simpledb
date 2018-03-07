package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private class Group {
        Field field;
        int count;

        Group(Field field) {
            this.field = field;
            this.count = 0;
        }
    }

    private class AggregatorIterator implements OpIterator {
        private static final long serialVersionUID = 1L;
        Iterator<Group> i = null;
        Iterable<Group> groups;
        TupleDesc td;

        AggregatorIterator(TupleDesc td, Iterable<Group> groups) {
            this.groups = groups;
            this.td = td;
        }

        public void open() {
            i = groups.iterator();
        }

        public void close() {
            i = null;
        }

        public void rewind() {
            close();
            open();
        }

        public TupleDesc getTupleDesc() {
            return td;
        }

        public boolean hasNext() {
            return i.hasNext();
        }

        public Tuple next() {
            Group g = i.next();
            Tuple t = new Tuple(this.td);
            if (td.numFields() == 2) {
              t.setField(0, g.field);
            }
            if (td.numFields() == 2) {
              t.setField(1, new IntField(g.count));
            } else {
              t.setField(0, new IntField(g.count));
            }
            return t;
        }
    }

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ArrayList<Group> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new ArrayList<Group>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO:
        //     clean code
        // some code goes here
        if (gbfield == NO_GROUPING || gbfieldtype == null) {
            if (groups.size() == 0) {
                groups.add(new Group(null));
            }
            Group g = groups.get(0);
            g.count++;
            return ;
        }

        for (Group g: groups) {
            if (g.field.equals(tup.getField(gbfield))) {
                g.count++;
                return;
            }
        }

        Group g = new Group(tup.getField(gbfield));
        g.count++;
        groups.add(g);
        return ;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        Type[] ts;
        if (this.gbfield == NO_GROUPING || this.gbfieldtype == null) {
          ts = new Type[] { Type.INT_TYPE };
        } else {
          ts = new Type[] { this.gbfieldtype, Type.INT_TYPE };
        }
        TupleDesc td = new TupleDesc(ts);
        return new AggregatorIterator(td, this.groups);
    }

}
