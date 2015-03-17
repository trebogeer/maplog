package com.trebogeer.log;

import java.io.File;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:32 PM
 */
public class FileLogConfig extends LogConfig {

    private static final String FILE_LOG_DIRECTORY = "directory";

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
        //this.config = config.withValue(FILE_LOG_DIRECTORY, ConfigValueFactory.fromAnyRef(Assert.isNotNull(directory, "directory")));
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
        return new File(System.getProperty("user.home") + "/tmp/");
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

    /*@Override
    public Log getLogManager(String name) {
        return new FileLog(name, this);
    }*/

}
