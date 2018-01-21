package simpledb.histogram;

import simpledb.operator.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;

    private int totalCounts;
    private int[] histogram;
    private int step;

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
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.max = max;
        this.min = min;

        this.histogram = new int[buckets];
        if (max - min > buckets) {
            this.step = (max - min - 1 + buckets)  / buckets;
        } else {
            this.step = 1;
        }
        this.totalCounts = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max) {
            return;
        }

        int index = (v - min) / step;
        if (index < 0) {
            index = 0;
        } else if (index > histogram.length - 1) {
            index = histogram.length - 1;
        }

        histogram[index]++;
        totalCounts++;
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

    	// some code goes here
        if (op == null) {
            return 0.0;
        }
        int index;
        if (v < min) {
            index = 0;
        } else if (v > max) {
            index = histogram.length - 1;
        } else {
            index = (v - min) / step;
            if (index < 0) {
                index = 0;
            } else if (index > histogram.length - 1) {
                index = histogram.length - 1;
            }
        }

        int counts = histogram[index];
        double selectivity = 0.0;
        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    return 0.0;
                } else {
                    selectivity = counts * 1.0 / totalCounts;
                }
                break;
            case NOT_EQUALS:
                if (v < min || v > max) {
                    return 1.0;
                } else {
                    selectivity = (totalCounts - counts) * 1.0 / totalCounts;
                }
                break;
            case GREATER_THAN:
                if (v > max) {
                    return 0.0;
                }

                for (int i = index; i < histogram.length; i++) {
                    selectivity += histogram[i];
                }
                selectivity /= totalCounts;
                break;
            case GREATER_THAN_OR_EQ:
                if (v > max) {
                    return 0.0;
                }

                for (int i = index; i < histogram.length; i++) {
                    selectivity += histogram[i];
                }
                selectivity /= totalCounts;
                break;
            case LESS_THAN:
                if (v > max) {
                    index = histogram.length - 1;
                } else {
                    index = index - 1;
                }
                for (int i = index; i >= 0; i--) {
                    selectivity += histogram[i];
                }
                selectivity /= totalCounts;
                break;
            case LESS_THAN_OR_EQ:
                if (v < min) {
                    return 0;
                }

                for (int i = index; i >= 0; i--) {
                    selectivity += histogram[i];
                }
                selectivity /= totalCounts;
                break;
            case LIKE:
                break;

                default:
                    return 0.0;
        }
        return selectivity;
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
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + buckets +
                ", min=" + min +
                ", max=" + max +
                ", histogram=" + Arrays.toString(histogram) +
                ", step=" + step +
                '}';
    }
}
