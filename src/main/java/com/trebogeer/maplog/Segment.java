package com.trebogeer.maplog;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 11:45 AM
 */
public interface Segment extends Loggable, Serializable {

    /**
     * Returns the parent log.
     *
     * @return The parent log.
     */
    Log log();

    /**
     * Returns the segment id.
     *
     * @return the segment id.
     */
    short id();

    /**
     * Returns the segment timestamp.
     *
     * @return The segment timestamp.
     */
    long timestamp();

    ByteBuffer getEntry(long pos, int offset);

    /**
     * Compacts segment
     */
    void compact();

    /**
     * Catches up with modifications from other members
     */
    void catchUp();


}
