package com.trebogeer.maplog;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 11:51 AM
 */
public class LogConfig {


    private static final String LOG_SEGMENT_SIZE = "jlog.segment.size";
    private static final String LOG_SEGMENT_INTERVAL = "jlog.segment.interval";
    private static final String LOG_FLUSH_ON_WRITE = "jlog.flush.on-write";
    private static final String LOG_FLUSH_INTERVAL = "jlog.flush.interval";

    private static final String DEFAULT_CONFIGURATION = "log-defaults";
    private static final String CONFIGURATION = "log";

    private int segmentSize = Integer.getInteger(LOG_SEGMENT_SIZE, Integer.MAX_VALUE);
    private boolean flushOnWrite = Boolean.getBoolean(LOG_FLUSH_ON_WRITE);


    // TODO implement
    public LogConfig copy() {
        return this;
    }

    /**
     * Sets the log segment size in bytes.
     *
     * @param segmentSize The log segment size in bytes.
     * @throws java.lang.IllegalArgumentException If the segment size is not positive
     */
    public void setSegmentSize(int segmentSize) {
        if (segmentSize <= 0) throw new IllegalArgumentException("Segment size cannot be negative or 0.");
        this.segmentSize = segmentSize;
    }

    /**
     * Returns the log segment size in bytes.
     *
     * @return The log segment size in bytes.
     */
    public int getSegmentSize() {
        return this.segmentSize;
    }

    /**
     * Sets the log segment size, returning the log configuration for method chaining.
     *
     * @param segmentSize The log segment size.
     * @return The log configuration.
     * @throws java.lang.IllegalArgumentException If the segment size is not positive
     */
    public LogConfig withSegmentSize(int segmentSize) {
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
        return Long.MAX_VALUE;
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


}
