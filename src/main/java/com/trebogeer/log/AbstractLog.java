package com.trebogeer.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:14 PM
 */
public abstract class AbstractLog implements Loggable, Log<Long> {

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private LogConfig config;
    protected final TreeMap<Short, Segment> segments = new TreeMap<>();
    protected final Index<Long> index = new ConcurrentHashMapIndex();
    protected Segment currentSegment;
    private short nextSegmentId;
    private long lastFlush;
    protected String name;

    protected AbstractLog(LogConfig config) {
        this.config = config.copy();
    }

    @Override
    public LogConfig config() {
        return config;
    }


    @Override
    public Index<Long> index() {
        return index;
    }

    /**
     * Loads all log segments.
     *
     * @return A collection of all existing log segments.
     */
    protected abstract Collection<Segment> loadSegments();

    /**
     * Creates a new log segment.
     *
     * @param segmentId The log segment id.
     * @return A new log segment.
     */
    protected abstract Segment createSegment(short segmentId);

    /**
     * Returns a collection of log segments.
     */
    @Override
    public TreeMap<Short, Segment> segments() {
        return segments;
    }

    /**
     * Returns the current log segment.
     */
    @Override
    public Segment segment() {
        return currentSegment;
    }

    /**
     * Returns a log segment by index.
     *
     * @throws IndexOutOfBoundsException if no segment exists for the {@code index}
     */
    @Override
    public Segment segment(short index) {
        assertIsOpen();
        Map.Entry<Short, Segment> segment = segments.floorEntry(index);
        //assertContainsIndex(index);
        return segment.getValue();
    }

    /**
     * Returns the first log segment.
     */
    @Override
    public Segment firstSegment() {
        assertIsOpen();
        Map.Entry<Short, Segment> segment = segments.firstEntry();
        return segment != null ? segment.getValue() : null;
    }

    /**
     * Returns the last log segment.
     */
    @Override
    public Segment lastSegment() {
        assertIsOpen();
        Map.Entry<Short, Segment> segment = segments.lastEntry();
        return segment != null ? segment.getValue() : null;
    }

    @Override
    public synchronized void open() throws IOException {
        assertIsNotOpen();

        // Load existing log segments from disk.
        for (Segment segment : loadSegments()) {
            segment.open();
            segments.put(segment.id(), segment);
        }

        // If a segment doesn't already exist, create an initial segment starting at index 1.
        if (!segments.isEmpty()) {
            currentSegment = segments.lastEntry().getValue();
            nextSegmentId = currentSegment.id();
        } else {
            createInitialSegment();
        }

        clean();
    }

    /**
     * Cleans the log at startup.
     * <p>
     * In the event that a failure occurred during compaction, it's possible that the log could contain a significant
     * gap in indexes between segments. When the log is opened, check segments to ensure that first and last indexes of
     * each segment agree with other segments in the log. If not, remove all segments that appear prior to any index gap.
     */
    private void clean() throws IOException {
        // TODO figure out sanity check with random keys
        /*Long lastIndex = null;
        Long compactIndex = null;
        for (Segment segment : segments.values()) {
            if (segment.id() != segments.lastKey() && (lastIndex == null || segment.id() > lastIndex + 1)) {
                compactIndex = segment.index();
            }
            lastIndex = segment.lastIndex();
        }

        if (compactIndex != null) {
            compact(compactIndex);
        }*/
    }

    @Override
    public boolean isOpen() {
        return currentSegment != null;
    }

    @Override
    public long size() {
        assertIsOpen();
        return segments.values().stream().mapToLong(Segment::size).sum();
    }

//    @Override
//    public long entryCount() {
//        assertIsOpen();
//        return segments.values().stream().mapToLong(Segment::entryCount).sum();
//    }

    @Override
    public boolean isEmpty() {
        assertIsOpen();
        Segment firstSegment = firstSegment();
        return firstSegment == null || firstSegment.size() == 0;
    }

    @Override
    public byte[] appendEntry(ByteBuffer entry, byte[] index) throws IOException {
        assertIsOpen();
        checkRollOver();
        return currentSegment.appendEntry(entry, index);
    }

    /**
     * Returns the entry for the {@code index} by checking the current segment first, then looking up
     * the correct segment.
     */
    @Override
    public ByteBuffer getEntry(byte[] index) {
        assertIsOpen();
        Index.Value v = index().get(MurMur3.MurmurHash3_x64_64(index, 127));
        if (v != null) {
           return segment(v.segmentId).getEntry(v.position, v.offset);
        }
        return null;
    }


    @Override
    public void rollOver(short index) throws IOException {
        // If the current segment is empty then just remove it.
        if (currentSegment.isEmpty()) {
            segments.remove(currentSegment.id());
            currentSegment.close();
            currentSegment.delete();
            currentSegment = null;
        } else {
            currentSegment.flush();
        }

        currentSegment = createSegment(++nextSegmentId);
        logger.info("Rolling over to new segment at new index {}", index);

        // Open the new segment.
        currentSegment.open();

        segments.put(index, currentSegment);

        // Reset the segment flush time and check whether old segments need to be deleted.
        lastFlush = System.currentTimeMillis();
    }

    @Override
    public void compact(short index) throws IOException {
        // Assert.index(index, index >= index() && (lastIndex() == null || index <= lastIndex()), "%s is invalid for the log", index);
        // Assert.arg(index, segments.containsKey(index), "%s must be the first index of a segment", index);

        // Iterate through all segments in the log. If a segment's first index matches the given index or its last index
        // is less than the given index then remove/close/delete the segment.
        logger.info("Compacting log at index {}", index);
        for (Iterator<Map.Entry<Short, Segment>> iterator = segments.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<Short, Segment> entry = iterator.next();
            Segment segment = entry.getValue();
            if (index > segment.id()) {
                iterator.remove();
                segment.close();
                segment.delete();
            }
        }
    }

    @Override
    public void flush() {
        assertIsOpen();
        // Only flush the current segment is flush-on-write is enabled or the flush timeout has passed since the last flush.
        // Flushes will be attempted each time the algorithm is done writing entries to the log.
        if (config.isFlushOnWrite()) {
            currentSegment.flush();
        } else if (System.currentTimeMillis() - lastFlush > config.getFlushInterval()) {
            currentSegment.flush();
            lastFlush = System.currentTimeMillis();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        logger.info("Closing log file [{}]", name);
        for (Segment segment : segments.values())
            segment.close();
        segments.clear();
        currentSegment = null;
        logger.info("Closed log file [{}]", name);
    }

    @Override
    public boolean isClosed() {
        return currentSegment == null;
    }

    @Override
    public void delete() {
        segments.values().forEach(com.trebogeer.log.Segment::delete);
        segments.clear();
    }

    @Override
    public String toString() {
        return segments.toString();
    }

    /**
     * Creates an initial segment for the log.
     *
     * @throws IOException If the segment could not be opened.
     */
    private void createInitialSegment() throws IOException {
        currentSegment = createSegment(++nextSegmentId);
        currentSegment.open();
        segments.put((short) 1, currentSegment);
    }

    /**
     * Checks whether the current segment needs to be rolled over to a new segment.
     *
     * @throws LogException if a new segment cannot be opened
     */
    private void checkRollOver() throws IOException {
        if (currentSegment.isEmpty())
            return;

        boolean segmentSizeExceeded = currentSegment.size() >= config.getSegmentSize();
        boolean segmentExpired = config.getSegmentInterval() < Long.MAX_VALUE
                && System.currentTimeMillis() > currentSegment.timestamp() + config.getSegmentInterval();

        if (segmentSizeExceeded || segmentExpired) {
            rollOver((short) (currentSegment.id() + 1));
        }
    }


}
