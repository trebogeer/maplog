package com.trebogeer.maplog;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author dimav
 *         Date: 3/20/15
 *         Time: 3:58 PM
 */
public class NonLockingFileSegment extends FileSegment {


    NonLockingFileSegment(FileLog log, short id) {
        super(log, id);
    }

    @Override
    public byte[] appendEntry(ByteBuffer entry, byte[] index) {
        assertIsOpen();
        try {
            entry.rewind();

            long position = logFileChannel.position();
            logFileChannel.write(entry);
            storePosition(index, position, entry.limit());
            isEmpty = false;
        } catch (IOException e) {
            throw new LogException("error appending entry", e);
        }
        return index;
    }

    /**
     * Stores the position of an entry in the log.
     */
    protected void storePosition(byte[] index, long position, int offset) {
        try {
            long key = MurMur3.MurmurHash3_x64_64(index, 127);
            ByteBuffer buffer = indexBuffer.get().
                    putLong(key).putLong(position).putInt(offset);
            buffer.flip();
            indexFileChannel.write(buffer);
            log().index().put(key, new Index.Value(position, offset, id()));
        } catch (IOException e) {
            throw new LogException("error storing position", e);
        }
    }
}
