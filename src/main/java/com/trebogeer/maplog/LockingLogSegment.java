package com.trebogeer.maplog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author dimav
 *         Date: 3/25/15
 *         Time: 10:27 AM
 */
public class LockingLogSegment extends File0LogSegment {

    LockingLogSegment(FileLog log, short id) {
        super(log, id);
    }

    @Override
    public synchronized void open() throws IOException {
        super.open();
    }

    @Override
    public byte[] appendEntry(ByteBuffer entry, byte[] index, byte flags) {
        assertIsOpen();
        lock.lock();
        // TODO see if locking a region helps anyhow
        // TODO see if locking a region of a specific entry possible and helps
        try (FileLock fl = logWriteFileChannel.lock();
             FileLock il = indexWriteFileChannel.lock()) {
            entry.rewind();
            int size;
            long p;

            size = logWriteFileChannel.write(entry);
            p = logWriteFileChannel.position();
            maxSeenPosition.set(p);
            storePosition(index, p - size, size, flags);

            isEmpty = false;
            flush();
        } catch (IOException e) {
            throw new LogException("error appending entry", e);
        } finally {
            lock.unlock();
        }
        return index;
    }


    /**
     * Appends entries to the log with specified ids. Flushes logs at the end.
     *
     * @param entries The entries to append.
     * @return The successfully appended entries list.
     * @throws IllegalStateException If the log is not open.
     * @throws NullPointerException  If the entry is null.
     * @throws java.io.IOException   If a new segment cannot be opened
     */
    @Override
    public List<byte[]> appendEntries(Map<byte[], Entry> entries) throws IOException {
        if (entries == null || entries.isEmpty()) return null;
        assertIsOpen();
        lock.lock();
        List<byte[]> keys = new LinkedList<>();
        try (FileLock fl = logWriteFileChannel.lock();
             FileLock il = indexWriteFileChannel.lock()) {
            long position = logWriteFileChannel.position();
            for (Map.Entry<byte[], Entry> entry : entries.entrySet()) {
                Entry value;
                if (entry != null && (value = entry.getValue()) != null && entry.getKey() != null) {
                    ByteBuffer buffer = value.getEntry();
                    buffer.rewind();
                    int size;
                    size = logWriteFileChannel.write(buffer);
                    position = position + size;
                    maxSeenPosition.set(position);
                    storePosition(entry.getKey(), position - size, size, value.getMeta());
                    isEmpty = false;

                    keys.add(entry.getKey());
                }
            }
            flush();
        } catch (IOException e) {
            logger.error("Error appending entries: ", e);
            throw e;
        } finally {
            lock.unlock();
        }
        return keys;
    }

}
