package com.example.externalsort.aggregator;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.MappedByteBuffer;
import java.util.List;

/**
 * The class is a version of {@link BufferAggregator} extended to support {@link MappedByteBuffer} specific
 * functionality.
 *
 * @author Ruslan Sverchkov
 */
@ThreadSafe
public class MappedBufferAggregator extends BufferAggregator<MappedByteBuffer> {

    /**
     * Constructs a MappedBufferAggregator instance.
     *
     * @param buffers the buffers to operate on
     * @throws IllegalArgumentException if:
     *                                  * buffers list is null
     *                                  * buffers list contains null elements
     *                                  * one of the buffers position is not 0
     *                                  * one of the buffers limit is not a multiple of {@code Integer.SIZE / Byte.SIZE}
     */
    public MappedBufferAggregator(List<MappedByteBuffer> buffers) {
        super(buffers);
    }

    /**
     * Forces any changes made to this aggregator's content to be written to the storage device containing the mapped
     * file.
     *
     * @see {@link MappedByteBuffer#force()}
     */
    public void force() {
        for (MappedByteBuffer buffer : getBuffers()) {
            buffer.force();
        }
    }

}