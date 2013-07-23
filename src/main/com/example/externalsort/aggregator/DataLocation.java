package com.example.externalsort.aggregator;

import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.Immutable;
import java.nio.ByteBuffer;

/**
 * The class is intended to hold information about the location of data in the aggregator.
 * The only reason for it to exist is that I need to return two values from
 * {@link BufferAggregator#getDataLocation(long)} method.
 */
@Immutable
public class DataLocation {

    private final ByteBuffer buffer;
    private final int index;

    /**
     * Constructs a DataLocation instance.
     *
     * @param buffer a buffer
     * @param index  an index in the buffer
     * @throws IllegalArgumentException if:
     *                                  * buffer is null
     *                                  * index is negative
     *                                  * index is out of bounds of the buffer
     */
    public DataLocation(ByteBuffer buffer, int index) {
        Validate.notNull(buffer);
        Validate.isTrue(index >= 0);
        Validate.isTrue(index < buffer.limit() / BufferAggregator.INT_SIZE_IN_BYTES);
        this.buffer = buffer;
        this.index = index;
    }

    /**
     * Buffer getter.
     *
     * @return the buffer, never returns {@code null}
     */
    public ByteBuffer getBuffer() {
        return buffer;
    }

    /**
     * Index getter.
     *
     * @return the index in the buffer,
     *         always between 0 and ([buffer limit] / [int size]) - 1
     */
    public int getIndex() {
        return index;
    }

}