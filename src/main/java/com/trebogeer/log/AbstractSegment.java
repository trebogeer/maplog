package com.trebogeer.log;

import java.util.Comparator;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:38 PM
 */
public abstract class AbstractSegment implements Segment, Comparator<AbstractSegment> {

    protected final short id;

    protected AbstractSegment(short id) {
        this.id = id;
    }

    @Override
    public int compare(AbstractSegment a, AbstractSegment b) {
        return Long.compare(a.id, b.id);
    }

    @Override
    public short id() {
        return id;
    }


    @Override
    public String toString() {
        return String.valueOf(id());
    }

}
