package com.trebogeer.log;

import java.io.Serializable;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:38 PM
 */
public abstract class AbstractSegment implements Loggable, Segment, Comparator<AbstractSegment> {

    protected final long id;
    protected final static ConcurrentHashMap<Long, AbstractSegment.Value> memIndex = new ConcurrentHashMap<>();

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

    static final class Value implements Serializable {
        final long position;
        final int offset;

        public Value(long position, int offset) {
            this.position = position;
            this.offset = offset;
        }
    }
}
