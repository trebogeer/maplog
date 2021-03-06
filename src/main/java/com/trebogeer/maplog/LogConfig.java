package com.trebogeer.maplog;

import com.trebogeer.maplog.checksum.Checksum;
import com.trebogeer.maplog.checksum.Checksums;
import com.trebogeer.maplog.hash.Hash;
import com.trebogeer.maplog.hash.MurMur3;

import java.util.function.Supplier;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 11:51 AM
 */
public class LogConfig {


    private static final String LOG_SEGMENT_SIZE = "jlog.segment.size";
    private static final String LOG_SEGMENT_INTERVAL = "jlog.segment.interval";
    private static final String LOG_FLUSH_ON_WRITE = "jlog.flush.on-write";
    private static final String LOG_METRICS = "jlog.metrics";
    private static final String LOG_FLUSH_INTERVAL = "jlog.flush.interval";

    private static final String DEFAULT_CONFIGURATION = "log-defaults";
    private static final String CONFIGURATION = "log";

    private long segmentSize = Long.getLong(LOG_SEGMENT_SIZE, Integer.MAX_VALUE);
    private boolean flushOnWrite = System.getProperty(LOG_FLUSH_ON_WRITE) == null || Boolean.getBoolean(LOG_FLUSH_ON_WRITE);
    private boolean mertics = System.getProperty(LOG_METRICS) == null || Boolean.getBoolean(LOG_METRICS);
    private Supplier<Hash> hashSupplier = MurMur3.murmur3();
    private Checksum checksum = Checksums.crc32();

    public LogConfig(LogConfig other) {
        this.segmentSize = other.segmentSize;
        this.flushOnWrite = other.flushOnWrite;
        this.mertics = other.mertics;
        this.hashSupplier = other.hashSupplier;
        this.checksum = other.checksum;
    }


    public LogConfig copy() {
        return new LogConfig(this);
    }

    public LogConfig() {
    }

    /**
     * Sets the log segment size in bytes.
     *
     * @param segmentSize The log segment size in bytes.
     * @throws java.lang.IllegalArgumentException If the segment size is not positive
     */
    public void setSegmentSize(long segmentSize) {
        if (segmentSize <= 0) throw new IllegalArgumentException("Segment size cannot be negative or 0.");
        this.segmentSize = segmentSize;
    }

    /**
     * Returns the log segment size in bytes.
     *
     * @return The log segment size in bytes.
     */
    public long getSegmentSize() {
        return this.segmentSize;
    }

    /**
     * Sets the log segment size, returning the log configuration for method chaining.
     *
     * @param segmentSize The log segment size.
     * @return The log configuration.
     * @throws java.lang.IllegalArgumentException If the segment size is not positive
     */
    public LogConfig withSegmentSize(long segmentSize) {
        setSegmentSize(segmentSize);
        return this;
    }

    /**
     * Sets the log segment interval.
     *
     * @param segmentInterval The log segment interval.
     * @throws java.lang.IllegalArgumentException If the segment interval is not positive
     */
    public void setSegmentInterval(long segmentInterval) {

    }

    /**
     * Returns the log segment interval.
     *
     * @return The log segment interval.
     */
    public long getSegmentInterval() {
        return 0;
    }

    /**
     * Sets the log segment interval, returning the log configuration for method chaining.
     *
     * @param segmentInterval The log segment interval.
     * @return The log configuration.
     * @throws java.lang.IllegalArgumentException If the segment interval is not positive
     */
    public LogConfig withSegmentInterval(long segmentInterval) {
        setSegmentInterval(segmentInterval);
        return this;
    }

    /**
     * Sets whether to flush the log to disk on every write.
     *
     * @param flushOnWrite Whether to flush the log to disk on every write.
     */
    public void setFlushOnWrite(boolean flushOnWrite) {
        this.flushOnWrite = flushOnWrite;
    }

    /**
     * Returns whether to flush the log to disk on every write.
     *
     * @return Whether to flush the log to disk on every write.
     */
    public boolean isFlushOnWrite() {
        return this.flushOnWrite;
    }

    /**
     * Sets whether to flush the log to disk on every write, returning the log configuration for method chaining.
     *
     * @param flushOnWrite Whether to flush the log to disk on every write.
     * @return The log configuration.
     */
    public LogConfig withFlushOnWrite(boolean flushOnWrite) {
        setFlushOnWrite(flushOnWrite);
        return this;
    }

    /**
     * Sets the log flush interval.
     *
     * @param flushInterval The log flush interval.
     * @throws java.lang.IllegalArgumentException If the flush interval is not positive
     */
    public void setFlushInterval(long flushInterval) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    /**
     * Returns the log flush interval.
     *
     * @return The log flush interval.
     */
    public long getFlushInterval() {
        return Long.MAX_VALUE;
    }

    /**
     * Sets the log flush interval, returning the log configuration for method chaining.
     *
     * @param flushInterval The log flush interval.
     * @return The log configuration.
     * @throws java.lang.IllegalArgumentException If the flush interval is not positive
     */
    public LogConfig withFlushInterval(long flushInterval) {
        setFlushInterval(flushInterval);
        return this;
    }

    public boolean isMertics() {
        return mertics;
    }

    public void setMertics(boolean mertics) {
        this.mertics = mertics;
    }

    public LogConfig withMetrics(boolean metrics) {
        setMertics(metrics);
        return this;
    }

    public Supplier<Hash> getHashSupplier() {
        return hashSupplier;
    }

    public void setHashSupplier(Supplier<Hash> hashSupplier) {
        this.hashSupplier = hashSupplier;
    }

    public LogConfig withMetrics(Supplier<Hash> hashSupplier) {
        setHashSupplier(hashSupplier);
        return this;
    }

    public Checksum getChecksum() {
        return checksum;
    }

    public void setChecksum(Checksum checksum) {
        this.checksum = checksum;
    }

    public LogConfig withChecksum(Checksum c) {
        setChecksum(c);
        return this;
    }
}
