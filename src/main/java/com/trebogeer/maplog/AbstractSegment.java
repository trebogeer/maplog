package com.trebogeer.maplog;

import java.util.Comparator;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:38 PM
 */
public abstract class AbstractSegment implements Segment, Comparator<AbstractSegment> {

    protected final int id;

    protected AbstractSegment(int id) {
        this.id = id;
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
