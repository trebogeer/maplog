package com.trebogeer.maplog;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
     * @param entry     The entry to append.
     * @param key       The entry key.
     * @param metaFlags One byte of app level meta flags.
     * @return The appended entry index.
     * @throws IllegalStateException    If the log is not open.
     * @throws IllegalArgumentException If entry is invalid.
     * @throws NullPointerException     If the entry is null.
     * @throws java.io.IOException      If a new segment cannot be opened
     */
    byte[] appendEntry(ByteBuffer entry, byte[] key, byte metaFlags) throws IOException;


    /**
     * Appends an entry to the logger with specified id.
     *
     * @param entries The entries to append.
     * @return The appended entry index.
     * @throws IllegalStateException    If the log is not open.
     * @throws IllegalArgumentException If at least one of teh entries is invalid.
     * @throws NullPointerException     If the entry is null.
     * @throws java.io.IOException      If a new segment cannot be opened
     */
    List<byte[]> appendEntries(Map<byte[], Entry> entries) throws IOException;
//
//
//    byte[] deleteEntry(byte[] key) throws IOException;
//
//    List<byte[]> deleteEntries(Collection<byte[]> keys) throws IOException;

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

    public static final class Entry {
        ByteBuffer entry;
        byte meta;

        public Entry(ByteBuffer entry, byte flags) {
            this.entry = entry;
            this.meta = flags;
        }

        public ByteBuffer getEntry() {
            return entry;
        }

        public byte getMeta() {
            return meta;
        }
    }
}
