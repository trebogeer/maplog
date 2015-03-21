package com.trebogeer.maplog;

import java.io.File;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:32 PM
 */
public class FileLogConfig extends LogConfig {

    private static final String FILE_LOG_DIRECTORY = "jlog.dir";
    private static final String DENY_WRITES_AT_PERCENT = "jlog.stop.writes.at.percent";
    private static final String LOCK_FILES_ON_WRITE = "jlog.lock.files";

    private String logDir = System.getProperty(FILE_LOG_DIRECTORY, System.getProperty("user.dir"));
    private int stopWritesAtPercent = Integer.getInteger(DENY_WRITES_AT_PERCENT, 5);
    private boolean lockFiles = Boolean.getBoolean(LOCK_FILES_ON_WRITE);

    @Override
    public FileLogConfig copy() {
        return /*new FileLogConfig(this);*/this;
    }

    /**
     * Sets the log directory.
     *
     * @param directory The log directory.
     * @throws java.lang.NullPointerException If the directory is {@code null}
     */
    public void setDirectory(String directory) {
        logDir = directory;
    }

    /**
     * Sets the log directory.
     *
     * @param directory The log directory.
     * @throws java.lang.NullPointerException If the directory is {@code null}
     */
    public void setDirectory(File directory) {
        setDirectory(directory.getAbsolutePath());
    }

    /**
     * Returns the log directory.
     *
     * @return The log directory.
     */
    public File getDirectory() {
        return new File(logDir);
    }


    public int getStopWritesAtPercent() {
        return stopWritesAtPercent;
    }

    public void setStopWritesAtPercent(int stopWritesAtPercent) {
        if (stopWritesAtPercent < 0) throw new IllegalArgumentException();
        this.stopWritesAtPercent = stopWritesAtPercent;
    }

    /**
     * Sets the log directory, returning the log configuration for method chaining.
     *
     * @param directory The log directory.
     * @return The log configuration.
     * @throws java.lang.NullPointerException If the directory is {@code null}
     */
    public FileLogConfig withDirectory(String directory) {
        setDirectory(directory);
        return this;
    }

    /**
     * Sets the log directory, returning the log configuration for method chaining.
     *
     * @param directory The log directory.
     * @return The log configuration.
     * @throws java.lang.NullPointerException If the directory is {@code null}
     */
    public FileLogConfig withDirectory(File directory) {
        setDirectory(directory);
        return this;
    }

    @Override
    public FileLogConfig withSegmentSize(int segmentSize) {
        setSegmentSize(segmentSize);
        return this;
    }

    @Override
    public FileLogConfig withSegmentInterval(long segmentInterval) {
        setSegmentInterval(segmentInterval);
        return this;
    }

    @Override
    public FileLogConfig withFlushOnWrite(boolean flushOnWrite) {
        setFlushOnWrite(flushOnWrite);
        return this;
    }

    @Override
    public FileLogConfig withFlushInterval(long flushInterval) {
        setFlushInterval(flushInterval);
        return this;
    }


    public FileLogConfig withFlushInterval(int stopWritesAtPercent) {
        setStopWritesAtPercent(stopWritesAtPercent);
        return this;
    }

    public boolean isLockFiles() {
        return lockFiles;
    }

    public void setLockFiles(boolean lockFiles) {
        this.lockFiles = lockFiles;
    }

    public FileLogConfig withFileLocks(boolean lockFiles) {
        setLockFiles(lockFiles);
        return this;
    }
}
