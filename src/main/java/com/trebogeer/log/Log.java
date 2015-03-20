package com.trebogeer.log;

import java.io.IOException;
import java.io.Serializable;
import java.util.TreeMap;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 11:45 AM
 */
public interface Log<K> extends Serializable {

    /**
     * Returns the log configuration.
     *
     * @return The log configuration.
     */
    LogConfig config();

    /**
     * Returns a map of all segments in the log.
     *
     * @return A map of segments in the log.
     */
    TreeMap<Short, Segment> segments();

    /**
     * Returns the current log segment.
     */
    Segment segment();

    /**
     * Returns a log segment by index.
     *
     * @throws IndexOutOfBoundsException if no segment exists for the {@code index}
     */
    Segment segment(short index);

    /**
     * Returns the first log segment.
     */
    Segment firstSegment();

    /**
     * Returns the last log segment.
     */
    Segment lastSegment();

    /**
     * Forces the log to roll over to a new segment.
     *
     * @throws java.io.IOException If the log failed to create a new segment.
     */
    void rollOver(short index) throws IOException;

    /**
     * Compacts the log, removing all segments up to and including the given index.
     *
     * @param index The index to which to compact the log. This must be the first index of the last
     *              segment in the log to remove via compaction
     * @throws IllegalArgumentException  if {@code index} is not the first index of a segment or if
     *                                   {@code index} represents the last segment in the log
     * @throws IndexOutOfBoundsException if {@code index} is out of bounds for the log
     * @throws IOException               If the log failed to delete a segment.
     */
    void compact(short index) throws IOException;

    /**
     * Returns index
     *
     * @return index
     */
    Index<K> index();

    /**
     * Counts entries indexed.
     *
     * @return entry count
     */
    long entryCount();

}
