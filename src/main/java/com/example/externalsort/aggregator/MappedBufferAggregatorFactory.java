package com.example.externalsort.aggregator;

import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * The class is intended to construct a {@link MappedBufferAggregator instance} using the specified file.
 *
 * @author Ruslan Sverchkov
 */
@ThreadSafe
public class MappedBufferAggregatorFactory {

    private final int maxBytesToMap;

    /**
     * Constructs a MappedBufferAggregatorFactory instance.
     *
     * @param maxBytesToMap max number of bytes mapped by one mapped byte buffer
     * @throws IllegalArgumentException if:
     *                                  * maxBytesToMap is not positive
     *                                  * maxBytesToMap is not a multiple of {@link BufferAggregator#INT_SIZE_IN_BYTES}
     */
    public MappedBufferAggregatorFactory(int maxBytesToMap) {
        Validate.isTrue(maxBytesToMap > 0);
        Validate.isTrue(maxBytesToMap % BufferAggregator.INT_SIZE_IN_BYTES == 0);
        this.maxBytesToMap = maxBytesToMap;
    }

    /**
     * The method is intended to construct a {@link MappedBufferAggregator instance} using the specified file.
     *
     * @param file a file to construct a {@link MappedBufferAggregator instance} for
     * @return a {@link MappedBufferAggregator instance} constructed using the specified file, never returns null
     * @throws IOException if the given file object does not denote an existing, writable regular file and
     *                     a new regular file of that name cannot be created, or if some other error occurs
     *                     while opening or creating the file
     */
    public MappedBufferAggregator get(File file) throws IOException {
        Validate.notNull(file);
        Validate.isTrue(file.length() % 4 == 0);
        List<MappedByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < file.length() / maxBytesToMap; i++) {
            buffers.add(getMappedByteBuffer(file, maxBytesToMap * i, maxBytesToMap));
        }
        int tail = (int) (file.length() % maxBytesToMap);
        if (tail != 0) {
            buffers.add(getMappedByteBuffer(file, file.length() - tail, tail));
        }
        return new MappedBufferAggregator(buffers);
    }

    /**
     * The method is intended to construct a {@link MappedByteBuffer} instance.
     *
     * @param file     a file to map
     * @param position The position within the file at which the mapped region
     *                 is to start; must be non-negative
     * @param size     The size of the region to be mapped; must be non-negative and
     *                 no greater than {@link java.lang.Integer#MAX_VALUE}
     * @return a {@link MappedByteBuffer} instance, never returns null
     * @throws IOException              if the given file object does not denote an existing, writable regular file and
     *                                  a new regular file of that name cannot be created, or if some other error occurs
     *                                  while opening or creating the file
     * @throws IllegalArgumentException If the preconditions on the parameters do not hold
     */
    protected MappedByteBuffer getMappedByteBuffer(File file, long position, long size) throws IOException {
        return new RandomAccessFile(file, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, position, size);
    }

}