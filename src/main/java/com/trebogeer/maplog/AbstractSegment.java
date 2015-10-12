package com.trebogeer.maplog;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.Function;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:38 PM
 */
public abstract class AbstractSegment implements Segment, Comparator<AbstractSegment> {

    protected final int id;
    protected Function<ByteBuffer, ByteBuffer> compactor = byteBuffer -> byteBuffer;

    protected AbstractSegment(int id) {
        this.id = id;
    }

    public AbstractSegment(int id, Function<ByteBuffer, ByteBuffer> compactor) {
        this.id = id;
        this.compactor = compactor;
    }

    @Override
    public int compare(AbstractSegment a, AbstractSegment b) {
        return Integer.compare(a.id, b.id);
    }

    @Override
    public int id() {
        return id;
    }


    @Override
    public String toString() {
        return String.valueOf(id());
    }

}
