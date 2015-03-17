package com.trebogeer.log;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:38 PM
 */
public abstract class AbstractSegment<T> implements Loggable, Segment, Comparator<AbstractSegment> {

    protected final long id;
    protected final ConcurrentHashMap<ByteArrayWrapper, T> memIndex = new ConcurrentHashMap<>();

    protected AbstractSegment(long id) {
        this.id = id;
    }

    @Override
    public int compare(AbstractSegment a, AbstractSegment b) {
        return Long.compare(a.id, b.id);
    }

    @Override
    public long id() {
        return id;
    }


    @Override
    public String toString() {
        return String.valueOf(id());
    }

    static final class ByteArrayWrapper implements Serializable {
        private final byte[] data;

        public ByteArrayWrapper(byte[] data) {
            if (data == null) {
                throw new NullPointerException();
            }
            this.data = data;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ByteArrayWrapper)) {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper) other).data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }
}
