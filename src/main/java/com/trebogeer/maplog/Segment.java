package com.trebogeer.maplog;

import com.trebogeer.maplog.index.Value;

import java.io.IOException;
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


    ByteBuffer getEntry(long k, Value v) throws IOException;

    /**
     * Compacts segment
     */
    void compact();

    /**
     * Catches up with modifications from other members
     */
    void catchUp();

    /**
     * Close segment write resources
     * @throws IOException
     */
    void closeWrite() throws IOException;


}
