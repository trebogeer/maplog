package com.trebogeer.maplog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

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

}
