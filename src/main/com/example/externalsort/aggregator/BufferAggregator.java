package com.example.externalsort.aggregator;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;

/**
 * The class is intended to aggregate {@link MappedByteBuffer} instances so that a client can work with them as if
 * they were one big buffer. The motivation for its existence is the restricted NIO API:
 * - client can only map {@code Integer.MAX_VALUE} bytes;
 * - indexation in buffers is byte-oriented so client must calculate an int index each time he wants to read or write
 * an int;
 * - index is an integer which means that client couldn't address more than {@code Integer.MAX_VALUE} integers even if
 * he could map a buffer bigger than {@code Integer.MAX_VALUE} bytes;
 * <p/>
 * Thread safety and correctness of class's logic in general is based on the fact that no one accesses the buffers it
 * operates on from the outside. So, please make sure the buffers you specify as a constructor argument are not used
 * anywhere else, otherwise the behavior of the class is unpredictable.
 * <p/>
 * The buffers are served in the order they are located in the list, for example if there are two 8-bytes buffers in the
 * list, the length of the BufferAggregator instance will be 4 (it contains 4 integers). If client tries to access data
 * with index 1, he gets the last four bytes of the first buffer; If client tries to access data with index 2, he gets
 * the first four bytes of the second buffer.
 *
 * @author Ruslan Sverchkov
 */
@ThreadSafe
public class BufferAggregator<T extends ByteBuffer> {

    public static final int INT_SIZE_IN_BYTES = Integer.SIZE / Byte.SIZE;

    private final List<T> buffers; // todo replace this with an array for performance's sake
    private final long length;
    private final boolean readOnly;

    /**
     * Constructs a BufferAggregator instance.
     *
     * @param buffers the buffers to operate on
     * @throws IllegalArgumentException if:
     *                                  * buffers list is null
     *                                  * buffers list contains null elements
     *                                  * one of the buffers position is not 0
     *                                  * one of the buffers limit is not a multiple of {@code Integer.SIZE / Byte.SIZE}
     */
    public BufferAggregator(List<? extends T> buffers) {
        Validate.noNullElements(buffers);
        Validate.notEmpty(buffers);
        long tempLength = 0;
        boolean tempReadOnly = false;
        for (ByteBuffer buffer : buffers) {
            Validate.isTrue(buffer.position() == 0);
            Validate.isTrue(buffer.limit() % INT_SIZE_IN_BYTES == 0);
            tempLength += buffer.limit() / INT_SIZE_IN_BYTES;
            if (buffer.isReadOnly()) {
                tempReadOnly = true;
            }
        }
        this.buffers = ImmutableList.copyOf(buffers);
        length = tempLength;
        readOnly = tempReadOnly;
    }

    /**
     * Tells whether or not this aggregator is read-only. The aggregator is read-only if at least one of the underlying
     * buffers is read-only.
     *
     * @return whether or not this aggregator is read-only
     */
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * The aggregator's length getter.
     *
     * @return the aggregator's length, non-negative
     */
    public long getLength() {
        return length;
    }

    /**
     * The method is intended to return an integer specified by the index.
     *
     * @param index an index of integer to return
     * @return an integer specified by the index
     * @throws IndexOutOfBoundsException if the specified index is not less than the aggregator length
     * @throws IllegalStateException     if the actual location of the data has not been found, that could happen either
     *                                   if the source code itself is incorrect or if the underlying buffers has been
     *                                   changed from the outside
     */
    public int getInt(final long index) {
        DataLocation location = getDataLocation(index);
        ByteBuffer buffer = location.getBuffer();
        int indexInBuffer = location.getIndex();
        return buffer.getInt(indexInBuffer * INT_SIZE_IN_BYTES);
    }

    /**
     * The method is intended to set an integer specified by the index.
     *
     * @param index an index of integer to return
     * @param value a value to set
     * @throws IndexOutOfBoundsException if the specified index is not less than the aggregator length
     * @throws IllegalStateException     if the actual location of the data has not been found, that could happen either
     *                                   if the source code itself is incorrect or if the underlying buffers has been
     *                                   changed from the outside
     * @throws java.nio.ReadOnlyBufferException
     *                                   if this aggregator is read-only
     */
    public void setInt(long index, int value) {
        DataLocation location = getDataLocation(index);
        ByteBuffer buffer = location.getBuffer();
        int indexInBuffer = location.getIndex();
        buffer.putInt(indexInBuffer * INT_SIZE_IN_BYTES, value);
    }

    /**
     * The method is intended to determine the actual location of the data requested by the specified index in the
     * underlying byte buffers.
     *
     * @param index index of the data requested
     * @return the actual location of the data requested, never returns {@code null}
     * @throws IndexOutOfBoundsException if the specified index is not less than the aggregator length
     * @throws IllegalStateException     if the actual location of the data has not been found, that could happen either
     *                                   if the source code itself is incorrect or if the underlying buffers has been
     *                                   changed from the outside
     */
    protected DataLocation getDataLocation(long index) {
        if (index >= length) {
            throw new IndexOutOfBoundsException("index is " + index + " and length is " + length);
        }
        long length = 0;
        for (T buffer : buffers) {
            long tempIndex = index - length;
            int intsInBuffer = buffer.limit() / INT_SIZE_IN_BYTES;
            if (tempIndex < intsInBuffer) {
                int indexInBuffer = (int) (tempIndex);
                return new DataLocation(buffer, indexInBuffer); // todo get rid of this for performance's sake
            }
            length += intsInBuffer;
        }
        throw new IllegalStateException("index validation has been failed");
    }

    /**
     * The method is intended to provide access to the aggregator internal data structures.
     * Pay attention that the returned list is immutable and an attempt to change it will lead to exception.
     *
     * @return buffers list, never returns null
     */
    protected List<T> getBuffers() {
        return buffers;
    }

}