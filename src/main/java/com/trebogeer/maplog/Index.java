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

        public Value(long position, int offset, short segmentId) {
            this.position = position;
            this.offset = offset;
            this.flags = 0;
            this.segmentId = segmentId;
        }


    }
}
