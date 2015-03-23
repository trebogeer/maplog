package com.trebogeer.maplog;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 11:45 AM
 */
public interface Loggable extends Closeable, Serializable {

    /**
     * Opens the logger.
     */
    void open() throws IOException;

    /**
     * Returns a boolean indicating whether the logger is empty.
     *
     * @return Indicates whether the logger is empty.
     */
    boolean isEmpty();

    /**
     * Returns a boolean indicating whether the log is open.
     *
     * @return Indicates whether the log is open.
     */
    boolean isOpen();

    /**
     * Returns the size in bytes.
     *
     * @return The size in bytes.
     * @throws IllegalStateException If the log is not open.
     */
    long size();


    /**
     * Appends an entry to the logger with specified id.
     *
     * @param entry The entry to append.
     * @param key The entry key.
     * @return The appended entry index.
     * @throws IllegalStateException If the log is not open.
     * @throws NullPointerException  If the entry is null.
     * @throws java.io.IOException   If a new segment cannot be opened
     */
    byte[] appendEntry(ByteBuffer entry, byte[] key) throws IOException;

    /**
     * Flushes the log to disk.
     *
     * @throws IllegalStateException If the log is not open.
     */
    void flush();

    /**
     * Closes the logger.
     */
    @Override
    void close() throws IOException;

    /**
     * Returns a boolean indicating whether the log is closed.
     *
     * @return Indicates whether the log is closed.
     */
    boolean isClosed();

    /**
     * Deletes the logger.
     */
    void delete();


    /**
     * Asserts whether the log is currently open.
     */
    default void assertIsOpen() {
        if (!isOpen())
            throw new IllegalStateException("The log is not currently open.");
    }

    /**
     * Asserts whether the log is currently closed.
     */
    default void assertIsNotOpen() {
        if (isOpen())
            throw new IllegalStateException("The log is already open.");
    }

//    /**
//     * Asserts whether the log contains the given index.
//     */
//    default void assertContainsIndex(long index) {
//        if (!containsIndex(index))
//            throw new IllegalStateException(String.format("Log does not contain index %d", index));
//    }

}
