package com.example.externalsort;

import com.example.externalsort.aggregator.BufferAggregator;
import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.RecursiveAction;

/**
 * A naive quick sort implementation. It works fine though.
 *
 * @author Ruslan Sverchkov
 */
@NotThreadSafe
public class SortTask extends RecursiveAction {

    private final BufferAggregator aggregator;
    private final long firstIndex;
    private final long lastIndex;

    /**
     * Constructs a SortTask instance.
     *
     * @param aggregator the aggregator to sort
     * @param firstIndex first index of sub-sequence to sort
     * @param lastIndex  last index of sub-sequence to sort, can be negative, which is funny, but this is forced by the
     *                   algorithm (lastIndex can be -1 in some cases), if lastIndex < firstIndex, which is always
     *                   non-negative, we simply do nothing
     * @throws IllegalArgumentException if:
     *                                  * aggregator is null
     *                                  * aggregator is read-only
     *                                  * first index is negative
     */
    public SortTask(BufferAggregator aggregator, long firstIndex, long lastIndex) {
        Validate.notNull(aggregator);
        Validate.isTrue(!aggregator.isReadOnly());
        Validate.isTrue(firstIndex >= 0);
        this.aggregator = aggregator;
        this.firstIndex = firstIndex;
        this.lastIndex = lastIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void compute() {
        if (firstIndex >= lastIndex) {
            return;
        }
        long i = firstIndex, j = lastIndex;
        int pivot = aggregator.getInt(firstIndex + (lastIndex - firstIndex) / 2);
        while (i <= j) {
            while (aggregator.getInt(i) < pivot) {
                i++;
            }
            while (aggregator.getInt(j) > pivot) {
                j--;
            }
            if (i <= j) {
                int temp = aggregator.getInt(i);
                aggregator.setInt(i, aggregator.getInt(j));
                aggregator.setInt(j, temp);
                i++;
                j--;
            }
        }
        SortTask left = new SortTask(aggregator, firstIndex, j);
        SortTask right = new SortTask(aggregator, i, lastIndex);
        invokeAll(left, right);
    }

}