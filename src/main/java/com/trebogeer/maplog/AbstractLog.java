package com.trebogeer.maplog;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.trebogeer.maplog.hash.Hash;
import com.trebogeer.maplog.index.ConditionalHashMapIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:14 PM
 */
public abstract class AbstractLog implements Loggable, Log<Long> {

    private static final Logger logger = LoggerFactory.getLogger("JLOG.F");

    // TODO move to config
    private static final String jmx_tld = System.getProperty("jlog.jmx.tld", "jlog.monitor");

    public static final MetricRegistry REGISTRY = new MetricRegistry();
    private static final JmxReporter reporter = JmxReporter.forRegistry(REGISTRY).inDomain(jmx_tld).build();
    private static final Slf4jReporter sl4jreproter = Slf4jReporter.forRegistry(REGISTRY).convertDurationsTo(TimeUnit.MICROSECONDS).outputTo(logger).convertRatesTo(TimeUnit.SECONDS).build();
    private static com.codahale.metrics.Timer writes = REGISTRY.timer("jlog.writes");
    private static com.codahale.metrics.Timer reads = REGISTRY.timer("jlog.reads");

    private LogConfig config;
    protected final TreeMap<Short, Segment> segments = new TreeMap<>();
    protected final Index<Long> index = new ConditionalHashMapIndex();
    protected Segment currentSegment;
    private short nextSegmentId;
    private long lastFlush;
    protected String name;
    protected final Hash hash;

    protected AbstractLog(LogConfig config) {
        this.config = config.copy();
        this.hash = config.getHashSupplier().get();
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
        if (config.isMertics()) {
            reporter.start();
            sl4jreproter.start(10, TimeUnit.SECONDS);
        }

        long start = System.currentTimeMillis();
        // having concurrent load does not make much sense on spinning drive. Sequential reads work better.
        // Setting concurrency level to one for now.
        ExecutorService es = Executors.newFixedThreadPool(/*Runtime.getRuntime().availableProcessors()*/1);
        // Load existing log segments from disk.
        Map<Short, Segment> syncSegments = Collections.synchronizedMap(segments);
        List<Callable<Short>> callables = new LinkedList<>();
        for (Segment segment : loadSegments()) {
            callables.add(() -> {
                segment.open();
                syncSegments.put(segment.id(), segment);
                return segment.id();
            });
        }

        try {

            List<Future<Short>> futures = es.invokeAll(callables);
            es.shutdown();
            if (!es.awaitTermination(10, TimeUnit.MINUTES)) {
                es.shutdownNow();
                logger.error("Failed to open segments within 5 minutes. Will not start.");
                throw new LogException("Failed to open segments within 5 minutes.");
            }

            for (Future<Short> res : futures) {
                try {
                    res.get();
                } catch (ExecutionException ee) {
                    logger.error("Failed to load segment due to error. Will not start.", ee);
                    throw new LogException("Failed to load segment due to error.", ee);
                }
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // If a segment doesn't already exist, create an initial segment starting at index 1.
        if (!segments.isEmpty()) {
            currentSegment = segments.lastEntry().getValue();
            nextSegmentId = currentSegment.id();
        } else {
            createInitialSegment();
        }

        clean();
        logger.info("Loaded {} index in memory. Elapsed time millis : {}, total number of entries : {}",
                name, System.currentTimeMillis() - start, index().size());
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

    Hash hash() {
        return this.hash;
    }

    @Override
    public long size() {
        assertIsOpen();
        return segments.values().stream().mapToLong(Segment::size).sum();
    }

    @Override
    public long entryCount() {
        assertIsOpen();
        return index().size();
    }

    @Override
    public boolean isEmpty() {
        assertIsOpen();
        Segment firstSegment = firstSegment();
        return firstSegment == null || firstSegment.size() == 0;
    }

    @Override
    public byte[] appendEntry(ByteBuffer entry, byte[] index, byte flags) throws IOException {
        long s = System.nanoTime();
        assertIsOpen();
        checkRollOver();
        byte[] b = currentSegment.appendEntry(entry, index, flags);
        writes.update(System.nanoTime() - s, TimeUnit.NANOSECONDS);
        return b;
    }

    /**
     * Returns the entry for the {@code index} by checking the current segment first, then looking up
     * the correct segment.
     */
    @Override
    public ByteBuffer getEntry(byte[] index) {
        assertIsOpen();
        ByteBuffer result = null;
        long start = System.nanoTime();
        Index.Value v = index().get(hash.hash(index));
        if (v != null) {
            result = segment(v.segmentId).getEntry(v.position, v.offset);
        }
        reads.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        return result;
    }


    @Override
    // TODO this needs to be optimized. may be rollover in advance as it's quite heavy.
    public synchronized void rollOver(short index) throws IOException {
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
        logger.info("Rolling over to new segment at index {}, total entries {}", index, index().size());

        // Open the new segment.
        currentSegment.open();

        segments.put(index, currentSegment);

        // Reset the segment flush time and check whether old segments need to be deleted.
        lastFlush = System.currentTimeMillis();
    }

    @Override
    // TODO employ bloom filter to check if compaction is needed at all?
    public void compact() throws IOException {
        for (Map.Entry<Short, Segment> entry : segments.entrySet()) {
            Segment segment = entry.getValue();
            if (segment != null && segment.id() != currentSegment.id()) {
                segment.compact();
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
        if (config.isMertics()) {
            reporter.stop();
            sl4jreproter.stop();
        }
        logger.info("Closed log file [{}]", name);
    }

    @Override
    public boolean isClosed() {
        return currentSegment == null;
    }

    @Override
    public void delete() {
        segments.values().forEach(com.trebogeer.maplog.Segment::delete);
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
     * @throws IOException if a new segment cannot be opened
     */
    private synchronized void checkRollOver() throws IOException {
        if (currentSegment == null || currentSegment.isEmpty())
            return;

        boolean segmentSizeExceeded = currentSegment.size() >= config.getSegmentSize();
        boolean segmentExpired = config.getSegmentInterval() < Long.MAX_VALUE
                && System.currentTimeMillis() > currentSegment.timestamp() + config.getSegmentInterval();

        if (segmentSizeExceeded || segmentExpired) {
            if (checkSpaceAvailable()) {
                rollOver((short) (currentSegment.id() + 1));
            } else {
                logger.error("Available space is beyond allowed minimum threshold. Disallowing further writes.");
                throw new IOException("Available space is beyond allowed minimum threshold. Disallowing further writes.");
            }
        }
    }

    /**
     * Checks space available on partition prior to rollover.
     */

    protected abstract boolean checkSpaceAvailable();

    /**
     * Gets a log name.
     *
     * @return name
     */
    @Override
    public String name() {
        return name;
    }
}
