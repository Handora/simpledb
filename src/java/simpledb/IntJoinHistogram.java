package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntJoinHistogram {
    private int numBuckets;
    private int min;
    private int max;
    private int numPerBuckets;
    private int[] buckets;
    private int ntups;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntJoinHistogram(int buckets, int min, int max) {
        this.numBuckets = buckets;
        this.max = max;
        this.min = min;
        this.numPerBuckets = (max - min + buckets) / buckets;

        this.buckets = new int[this.numBuckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v > max || v < min) {
          return ;
        }
        this.buckets[(v-this.min)/this.numPerBuckets]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        int index = (v-this.min)/this.numPerBuckets;
        if (op.equals(Predicate.Op.EQUALS)) {
            if (v < min) {
                return 0.0;
            }
            if (v > max) {
                return 0.0;
            }
            return (double)this.buckets[index]/this.numPerBuckets/this.ntups;
        } else if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            if (v < min) {
                return 1.0;
            }
            if (v > max) {
                return 0.0;
            }
            double sum = 0.0;
            double fraction = (double)this.buckets[index] / this.ntups;
            double part = (double)(this.min + ((v-this.min+this.numPerBuckets-1)/this.numPerBuckets) * this.numPerBuckets - v +
                            (op.equals(Predicate.Op.GREATER_THAN) ? 0 : 1 )) / this.numPerBuckets;
            sum += fraction * part;

            for (int i=index+1; i<this.numBuckets; i++) {
                sum += (double)this.buckets[i] / this.ntups;
            }
            return sum;
        } else if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            if (v < min) {
                return 0.0;
            }
            if (v > max) {
                return 1.0;
            }
            double sum = 0.0;
            double fraction = (double)this.buckets[index] / this.ntups;

            double part = (double)(v - (this.min + ((v-this.min)/this.numPerBuckets) * this.numPerBuckets) +
                            (op.equals(Predicate.Op.LESS_THAN) ? 0 : 1 )) / this.numPerBuckets;
            sum += fraction * part;

            for (int i=index-1; i>=0; i--) {
                sum += (double)this.buckets[i] / this.ntups;
            }
            return sum;
        } else if (op.equals(Predicate.Op.NOT_EQUALS)) {
            if (v < min) {
                return 1.0;
            }
            if (v > max) {
                return 1.0;
            }
            return 1.0 - (double)this.numPerBuckets*this.buckets[index]/this.ntups;
        } else {
            return 1.0;
        }
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {

        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String s = "";
        s += "[" + this.numBuckets + ", " + this.numPerBuckets + "]";
        if (this.numBuckets == 0) {
          return s;
        }
        s += "{" + this.buckets[0];
        for (int i=1; i<numBuckets; i++) {
          s += " ," + this.buckets[i];
        }
        s += "}";
        return s;
    }
}
