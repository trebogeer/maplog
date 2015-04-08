package com.trebogeer.maplog;

import com.trebogeer.maplog.checksum.Checksum;
import com.trebogeer.maplog.index.Value;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
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
    NavigableMap<Short, Segment> segments();

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
    void rollOver() throws IOException;

    /**
     * Compacts the log, removing obsolete entries.
     *
     * @throws IllegalArgumentException if {@code index} is not the first index of a segment or if
     *                                  {@code index} represents the last segment in the log
     * @throws IOException              If the log failed to compact a segment.
     */
    void compact() throws IOException;

    /**
     * Returns index
     *
     * @return index
     */
    Map<K, Value> index();

    /**
     * Returns checksum function for entry integrity check.
     * @return checksum
     */
    Checksum checksum();

    /**
     * Counts entries indexed.
     *
     * @return entry count
     */
    long entryCount();

    /**
     * Gets an entry from the log.
     *
     * @param key The index of the entry to get.
     * @return The entry at the given index, or {@code null} if the entry doesn't exist.
     * @throws IllegalStateException If the log is not open.
     */
    ByteBuffer getEntry(byte[] key) throws IOException;


    /**
     * Gets metadata flags by index key.
     *
     * @param key The index key of the entry to get flags for.
     * @return The meta flags at the given index, or {@code null} if the entry doesn't exist.
     * @throws java.lang.IllegalStateException if the log is not open.
     */
    Byte getMetaFlags(byte[] key);

    /**
     * Gets a log name.
     *
     * @return name
     */
    String name();


}
