package com.trebogeer.maplog;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.trebogeer.maplog.checksum.Checksum;
import com.trebogeer.maplog.hash.Hash;
import com.trebogeer.maplog.index.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
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

// TODO take a look at Zip File System Provider
// TODO add better check of input of appendEntry/appendEntries
public abstract class AbstractLog implements Loggable, Log<Long> {

    private static final Logger logger = LoggerFactory.getLogger("JLOG.F");

    // TODO move to config
    private static final String jmx_tld = System.getProperty("jlog.jmx.tld", "jlog.monitor");

    private static final MetricRegistry REGISTRY = new MetricRegistry();
    private static final JmxReporter reporter = JmxReporter.forRegistry(REGISTRY).inDomain(jmx_tld).build();
    private static final Slf4jReporter sl4jreproter = Slf4jReporter.forRegistry(REGISTRY).convertDurationsTo(TimeUnit.MICROSECONDS).outputTo(logger).convertRatesTo(TimeUnit.SECONDS).build();
    private static com.codahale.metrics.Timer writes = REGISTRY.timer("jlog.writes");
    private static com.codahale.metrics.Timer reads = REGISTRY.timer("jlog.reads");

    private LogConfig config;
    protected final ConcurrentSkipListMap<Integer, Segment> segments = new ConcurrentSkipListMap<>();
    protected final Map<Long, Value> index = new ConcurrentHashMap<>();
    protected Segment currentSegment;
    private int nextSegmentId;
    private long lastFlush;
    protected String name;
    protected final Hash hash;
    protected final Checksum checksum;

    protected AbstractLog(LogConfig config) {
        this.config = config.copy();
        this.hash = config.getHashSupplier().get();
        this.checksum = config.getChecksum();
    }

    @Override
    public LogConfig config() {
        return config;
    }

    @Override
    public Checksum checksum() {
        return this.checksum;
    }

    @Override
    public Map<Long, Value> index() {
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
    protected abstract Segment createSegment(int segmentId);

    /**
     * Returns a collection of log segments.
     */
    @Override
    public NavigableMap<Integer, Segment> segments() {
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
    public Segment segment(int index) {
        assertIsOpen();
        Map.Entry<Integer, Segment> segment = segments.floorEntry(index);
        return segment == null ? null : segment.getValue();
    }

    /**
     * Returns the first log segment.
     */
    @Override
    public Segment firstSegment() {
        assertIsOpen();
        Map.Entry<Integer, Segment> segment = segments.firstEntry();
        return segment != null ? segment.getValue() : null;
    }

    /**
     * Returns the last log segment.
     */
    @Override
    public Segment lastSegment() {
        assertIsOpen();
        Map.Entry<Integer, Segment> segment = segments.lastEntry();
        return segment != null ? segment.getValue() : null;
    }

    @Override
    // TODO check if compaction is in progress
    public synchronized void open() throws IOException {
        assertIsNotOpen();
        // TODO rework config for meters
        if (config.isMertics()) {
            reporter.start();
            sl4jreproter.start(10, TimeUnit.SECONDS);
        }

        long start = System.currentTimeMillis();
        // having concurrent load does not make much sense on spinning drive. Sequential reads work better.
        // Setting concurrency level to one for now.
        ExecutorService es = Executors.newFixedThreadPool(/*Runtime.getRuntime().availableProcessors()*/1);
        // Load existing log segments from disk.
        Map<Integer, Segment> syncSegments = Collections.synchronizedMap(segments);
        List<Callable<Integer>> callables = new LinkedList<>();
        for (Segment segment : loadSegments()) {
            callables.add(() -> {
                segment.open();
                syncSegments.put(segment.id(), segment);
                return segment.id();
            });
        }

        try {

            List<Future<Integer>> futures = es.invokeAll(callables);
            es.shutdown();
            if (!es.awaitTermination(10, TimeUnit.MINUTES)) {
                es.shutdownNow();
                logger.error("Failed to open segments within 5 minutes. Will not start.");
                throw new LogException("Failed to open segments within 5 minutes.");
            }

            for (Future<Integer> res : futures) {
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

        logger.info("Loaded {} index in memory. Elapsed time millis : {}, total number of entries : {}",
                name, System.currentTimeMillis() - start, index().size());
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
    // TODO optimize locks on in the update path
    public byte[] appendEntry(ByteBuffer entry, byte[] index, byte flags) throws IOException {
        long s = System.nanoTime();
        assertIsOpen();
        if (entry.limit() - entry.position() > (Integer.MAX_VALUE - Constants.CRC_AND_SIZE)) {
            throw new IllegalArgumentException(String.format("Entry size is too over allowed maximum value: " +
                            "size - %d bytes, max size - %d bytes", entry.limit() - entry.position(),
                    Integer.MAX_VALUE - Constants.CRC_AND_SIZE));
        }
        rollOver();
        byte[] b = currentSegment.appendEntry(entry, index, flags);
        writes.update(System.nanoTime() - s, TimeUnit.NANOSECONDS);
        return b;
    }

    @Override
    public List<byte[]> appendEntries(Map<byte[], Entry> entries) throws IOException {
        long s = System.nanoTime();
        assertIsOpen();
        for (Map.Entry<byte[], Entry> entry : entries.entrySet()) {
            ByteBuffer e = entry.getValue().getEntry();
            if (e.limit() - e.position() > (Integer.MAX_VALUE - Constants.CRC_AND_SIZE)) {
                throw new IllegalArgumentException(String.format("Entry size is too over allowed maximum value: " +
                                "size - %d bytes, max size - %d bytes", e.limit() - e.position(),
                        Integer.MAX_VALUE - Constants.CRC_AND_SIZE));
            }
        }
        rollOver();
        List<byte[]> b = currentSegment.appendEntries(entries);
        writes.update(System.nanoTime() - s, TimeUnit.NANOSECONDS);
        return b;
    }

    /**
     * Returns the entry for the {@code index} by checking the current segment first, then looking up
     * the correct segment.
     */
    @Override
    public ByteBuffer getEntry(byte[] index) throws IOException {
        if (index == null || index.length == 0) return null;
        assertIsOpen();
        ByteBuffer result = null;
        long start = System.nanoTime();
        long key = hash.hash(index);
        Value v = index().get(key);
        if (v != null) {
            result = segment(v.getSegmentId()).getEntry(key, v);
        }
        reads.update(System.nanoTime() - start, TimeUnit.NANOSECONDS);
        return result;
    }

    /**
     * Gets metadata flags by index key.
     *
     * @param key The index key of the entry to get flags for.
     * @return The meta flags at the given index, or {@code null} if the entry doesn't exist.
     * @throws IllegalStateException if the log is not open.
     */
    @Override
    public Byte getMetaFlags(byte[] key) {
        assertIsOpen();
        Value v = index.get(hash.hash(key));
        return v == null ? null : v.getFlags();
    }

    @Override
    // TODO this needs to be optimized.
    // TODO currentSegment.closeWrite() is unsafe to call. Need to complete all pending writes first.
    public synchronized void rollOver() throws IOException {

        if (currentSegment == null || currentSegment.isEmpty())
            return;

        boolean segmentSizeExceeded = currentSegment.size() >= config.getSegmentSize();

        if (segmentSizeExceeded) {
            if (checkSpaceAvailable()) {

                if (!currentSegment.isEmpty()) {
                    currentSegment.flush();
                }
                currentSegment.closeWrite();

                int newSegmentId = ++nextSegmentId;

                currentSegment = createSegment(newSegmentId);
                logger.info("Rolling over to new segment {}, total entries {}",
                        name() + "/" + currentSegment.id(), index().size());

                // Open the new segment.
                currentSegment.open();

                segments.put(newSegmentId, currentSegment);

                // Reset the segment flush time
                lastFlush = System.currentTimeMillis();

            } else {
                logger.error("Available space is beyond allowed minimum threshold. Disallowing further writes.");
                throw new IOException("Available space is beyond allowed minimum threshold. Disallowing further writes.");
            }
        }


    }

    @Override
    // TODO employ bloom filter to check if compaction is needed at all?
    public void compact() throws IOException {
        for (Map.Entry<Integer, Segment> entry : segments.entrySet()) {
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
        segments.put(1, currentSegment);
    }

    /**
     * Removes segment from log.
     *
     * @param index id to remove
     */
    @Override
    public Segment remove(int index) {
        return segments.remove(index);
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
