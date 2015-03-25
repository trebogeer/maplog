package com.trebogeer.maplog;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dimav
 *         Date: 3/25/15
 *         Time: 10:27 AM
 */
public class LockingLogSegment extends File0LogSegment {

    private AtomicLong lastWritePositionLog = new AtomicLong(0);
    private AtomicLong lastWritePositionIndex = new AtomicLong(0);

    LockingLogSegment(FileLog log, short id) {
        super(log, id);
    }

    @Override
    public synchronized void open() throws IOException {
        super.open();
        lastWritePositionLog.set(logWriteFileChannel.position());
        lastWritePositionIndex.set(indexWriteFileChannel.position());
    }

    @Override
    public byte[] appendEntry(ByteBuffer entry, byte[] index, byte flags) {
        assertIsOpen();
        lock.lock();
        long lpw = lastWritePositionLog.get();
        long lpi = lastWritePositionIndex.get();
        try (FileLock fl = logWriteFileChannel.lock(lpw, Long.MAX_VALUE - lpw - 1, false);
             FileLock il = indexWriteFileChannel.lock(lpi, Long.MAX_VALUE - lpi - 1, false)) {
            entry.rewind();
            int size;
            long p;

            size = logWriteFileChannel.write(entry);
            p = logWriteFileChannel.position();

            lastWritePositionLog.lazySet(p);
            storePosition(index, p - size, size, flags);
            // need no sync, approx position is ok for this purpose
            lastWritePositionIndex.lazySet(indexWriteFileChannel.position());
            isEmpty = false;
        } catch (IOException e) {
            throw new LogException("error appending entry", e);
        } finally {
            lock.unlock();
        }
        return index;
    }

}
