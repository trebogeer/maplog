package com.trebogeer.maplog;

import java.io.Serializable;
import java.util.Map;

/**
 * @author dimav
 *         Date: 3/18/15
 *         Time: 10:16 AM
 */
public interface Index<K> {

    void put(K k, Value v);

    Value get(K k);

    long size();

    void putAll(Map<K, Value> subset);

    static final class Value implements Serializable {
        final long position;
        final int offset;
        final byte flags;
        final short segmentId;

        public Value(long position, int offset, short segmentId, byte flags) {
            this.position = position;
            this.offset = offset;
            this.flags = flags;
            this.segmentId = segmentId;
        }

        public long getPosition() {
            return position;
        }

        public int getOffset() {
            return offset;
        }

        public byte getFlags() {
            return flags;
        }

        public short getSegmentId() {
            return segmentId;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Value)) return false;

            Value value = (Value) o;

            if (segmentId != value.segmentId) return false;
            if (offset != value.offset) return false;
            if (position != value.position) return false;
            if (flags != value.flags) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = (int) (position ^ (position >>> 32));
            result = 31 * result + offset;
            result = 31 * result + (int) flags;
            result = 31 * result + (int) segmentId;
            return result;
        }
    }
}
