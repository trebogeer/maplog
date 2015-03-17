package com.trebogeer.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;


/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:41 PM
 */
public class FileSegment extends AbstractSegment<FileSegment.Value> {

    private static final Logger logger = LoggerFactory.getLogger(FileSegment.class);

    private static final int INDEX_ENTRY_SIZE = 20;

    private final FileLog log;
    private final File logFile;
    private final File indexFile;
    private final File metadataFile;
    private long timestamp;
    private FileChannel logFileChannel;
    private FileChannel indexFileChannel;
    private boolean isEmpty = true;
    private final ThreadLocal<ByteBuffer> indexBuffer = new ThreadLocal<ByteBuffer>() {

        @Override
        protected ByteBuffer initialValue() {
            // TODO make size configurable
            return ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        }

        @Override
        public ByteBuffer get() {
            ByteBuffer bb = super.get();
            bb.rewind();
            return bb;
        }
    };

    FileSegment(FileLog log, long id) {
        super(id);
        this.log = log;
        this.logFile = new File(log.base.getParentFile(), String.format("%s-%d.log", log.base.getName(), id));
        this.indexFile = new File(log.base.getParentFile(), String.format("%s-%d.index", log.base.getName(), id));
        this.metadataFile = new File(log.base.getParentFile(), String.format("%s-%d.metadata", log.base.getName(), id));
    }

    @Override
    public Log log() {
        return log;
    }

    @Override
    public long timestamp() {
        assertIsOpen();
        return timestamp;
    }

    @Override
    public void open() throws IOException {
        assertIsNotOpen();
        if (!logFile.getParentFile().exists()) {
            logFile.getParentFile().mkdirs();
        }

        if (!metadataFile.exists()) {
            timestamp = System.currentTimeMillis();
            try (RandomAccessFile metaFile = new RandomAccessFile(metadataFile, "rw")) {
                metaFile.writeLong(super.id); // First index of the segment.
                metaFile.writeLong(timestamp); // Timestamp of the time at which the segment was created.
            }
        } else {
            try (RandomAccessFile metaFile = new RandomAccessFile(metadataFile, "r")) {
                long metaFileIndex = metaFile.readLong();
                if (metaFileIndex != super.id) {
                    throw new LogException("Segment metadata out of sync");
                }
                timestamp = metaFile.readLong();
            }
        }

        logFileChannel = FileChannel.open(this.logFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        logFileChannel.position(logFileChannel.size());
        indexFileChannel = FileChannel.open(this.indexFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long indexFileSize = indexFileChannel.size();
        if (indexFileSize != 0) {
            long start = System.currentTimeMillis();
            ByteBuffer b = indexBuffer.get();
            byte[] key = new byte[INDEX_ENTRY_SIZE - 12];
            while (indexFileChannel.read(b) != -1) {
                b.flip();
                b.get(key);
                memIndex.put(new ByteArrayWrapper(key), new Value(b.getLong(), b.getInt()));
                b.rewind();
            }
            logger.info("Loaded segment {} index in memory. Elapsed time millis : {}", id(), start - System.currentTimeMillis());
        }
        indexFileChannel.position(indexFileSize);

        if (indexFileSize > 0) {
            isEmpty = false;
        }
    }

    @Override
    public boolean isEmpty() {
        assertIsOpen();
        return isEmpty;
    }

    @Override
    public boolean isOpen() {
        return logFileChannel != null && indexFileChannel != null;
    }

    @Override
    public long size() {
        assertIsOpen();
        try {
            return logFileChannel.size();
        } catch (IOException e) {
            throw new LogException("error retrieving size from file channel", e);
        }
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
    private void storePosition(byte[] index, long position, int offset) {
        try {
            ByteBuffer buffer = indexBuffer.get()
                    .putLong(position).putLong(MurMur3.MurmurHash3_x64_64(index, 127)).putInt(offset);
            buffer.flip();
            indexFileChannel.write(buffer);
        } catch (IOException e) {
            throw new LogException("error storing position", e);
        }
    }


    /**
     * Finds the position of the given index in the segment.
     */
    private Value findPosition(byte[] index) {
        return memIndex.get(new ByteArrayWrapper(index));
    }


    @Override
    public ByteBuffer getEntry(byte[] index) {
        assertIsOpen();
        try {
            Value v = findPosition(index);
            if (v == null)
                return null;
            ByteBuffer buffer = ByteBuffer.allocate(v.offset);
            logFileChannel.read(buffer, v.position);
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            throw new LogException("error retrieving entry", e);
        }
    }


    @Override
    public void flush() {
        try {
            logFileChannel.force(false);
            indexFileChannel.force(false);
        } catch (IOException e) {
            throw new LogException("error ", e);
        }
    }

    @Override
    public void close() throws IOException {
        assertIsOpen();
        logFileChannel.close();
        logFileChannel = null;
        indexFileChannel.close();
        indexFileChannel = null;
    }

    @Override
    public boolean isClosed() {
        return logFileChannel == null;
    }

    @Override
    public void delete() {
        logFile.delete();
        indexFile.delete();
        metadataFile.delete();
    }

    static final class Value {
        final long position;
        final int offset;

        public Value(long position, int offset) {
            this.position = position;
            this.offset = offset;
        }
    }


}
