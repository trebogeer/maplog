package com.trebogeer.maplog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author dimav
 *         Date: 3/23/15
 *         Time: 2:32 PM
 */
public class File0LogSegment extends AbstractSegment {

    private static final Logger logger = LoggerFactory.getLogger("JLOG.F.SEGMENT");

    private static final int INDEX_ENTRY_SIZE = 20;

    private final FileLog log;
    private final File logFile;
    private final File indexFile;
    private final File metadataFile;
    private long timestamp;
    // protected FileChannel logFileChannel;
    protected FileChannel logReadFileChannel;
    protected FileChannel logWriteFileChannel;
    //protected FileChannel indexFileChannel;
    protected FileChannel indexReadFileChannel;
    protected FileChannel indexWriteFileChannel;
    protected boolean isEmpty = true;
    private static ReentrantLock lock = new ReentrantLock();
    protected final ThreadLocal<ByteBuffer> indexBuffer = new ThreadLocal<ByteBuffer>() {

        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(INDEX_ENTRY_SIZE);
        }

        @Override
        public ByteBuffer get() {
            ByteBuffer bb = super.get();
            bb.rewind();
            return bb;
        }
    };

    File0LogSegment(FileLog log, short id) {
        super(id);
        this.log = log;
        this.logFile = new File(log.base.getParentFile(), String.format("%s-%d.log", log.base.getName(), id));
        this.indexFile = new File(log.base.getParentFile(), String.format("%s-%d.index", log.base.getName(), id));
        this.metadataFile = new File(log.base.getParentFile(), String.format("%s-%d.metadata", log.base.getName(), id));
    }

    @Override
    public Log<Long> log() {
        return log;
    }

    @Override
    public long timestamp() {
        assertIsOpen();
        return timestamp;
    }

    @Override
    public synchronized void open() throws IOException {
        assertIsNotOpen();
        if (!logFile.getParentFile().exists()) {
            logFile.getParentFile().mkdirs();
        }

        if (!metadataFile.exists()) {
            timestamp = System.currentTimeMillis();
            try (RandomAccessFile metaFile = new RandomAccessFile(metadataFile, "rw")) {
                metaFile.writeShort(super.id);
                metaFile.writeLong(timestamp);
            }
        } else {
            try (RandomAccessFile metaFile = new RandomAccessFile(metadataFile, "r")) {
                short metaFileIndex = metaFile.readShort();
                if (metaFileIndex != super.id) {
                    throw new LogException("Segment metadata out of sync");
                }
                timestamp = metaFile.readLong();
            }
        }
        // write channel in append only mode

        logWriteFileChannel = FileChannel.open(this.logFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        logReadFileChannel = FileChannel.open(this.logFile.toPath(), StandardOpenOption.READ);

        logWriteFileChannel.position(logWriteFileChannel.size());
        logReadFileChannel.position(logReadFileChannel.size());

        indexWriteFileChannel = FileChannel.open(this.indexFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        indexReadFileChannel = FileChannel.open(this.indexFile.toPath(), StandardOpenOption.READ);

        long indexFileSize = indexReadFileChannel.size();
        if (indexFileSize != 0) {
            // TODO read bigger chunks here
            long start = System.currentTimeMillis();
            ByteBuffer b = indexBuffer.get();
            byte[] key = new byte[8];
            HashMap<Long, Index.Value> localIndex = new HashMap<>();
            while (indexReadFileChannel.read(b) != -1) {
                b.flip();
                b.get(key);
                localIndex.put(Utils.toLong(key), new Index.Value(b.getLong(), b.getInt(), id()));
                b.rewind();
            }
            // TODO wait on condition to add to global index.
            log().index().putAll(localIndex);
            logger.info("Loaded segment {} index in memory. Elapsed time millis : {}, total number of segment entries: {}",
                    id(), System.currentTimeMillis() - start, localIndex.size());
        }
        indexReadFileChannel.position(indexFileSize);

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
        return logWriteFileChannel != null && indexWriteFileChannel != null
                && logReadFileChannel != null && indexReadFileChannel != null;
    }

    @Override
    public long size() {
        assertIsOpen();
        try {
            // TODO optimize to return approx value may be. This is to estimate if rollover is needed mostly.
            return Math.max(logWriteFileChannel.position(), logReadFileChannel.position());
        } catch (IOException e) {
            throw new LogException("error retrieving size from file channel", e);
        }
    }


    @Override
    public byte[] appendEntry(ByteBuffer entry, byte[] index) {
        assertIsOpen();
        try {
            entry.rewind();
            lock.lock();
            int size = logWriteFileChannel.write(entry);
            storePosition(index, logWriteFileChannel.position() - size, size);
            isEmpty = false;
        } catch (IOException e) {
            throw new LogException("error appending entry", e);
        } finally {
            lock.unlock();
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
            indexWriteFileChannel.write(buffer);
            log().index().put(key, new Index.Value(position, offset, id()));
        } catch (IOException e) {
            throw new LogException("error storing position", e);
        }
    }

    @Override
    public ByteBuffer getEntry(long position, int offset) {
        assertIsOpen();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(offset /*- 4*/);
            logReadFileChannel.read(buffer, position /*+ 4*/);
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            throw new LogException("error retrieving entry", e);
        }
    }


    @Override
    public void flush() {
        try {
            logWriteFileChannel.force(false);
            indexWriteFileChannel.force(false);
        } catch (IOException e) {
            throw new LogException("error ", e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        logger.info("Closing segment [{}]", id());
        assertIsOpen();
        logWriteFileChannel.close();
        logWriteFileChannel = null;
        logReadFileChannel.close();
        logReadFileChannel = null;

        indexWriteFileChannel.close();
        indexWriteFileChannel = null;
        indexReadFileChannel.close();
        indexReadFileChannel = null;
    }

    @Override
    public boolean isClosed() {
        return logReadFileChannel == null;
    }

    @Override
    public void delete() {
        logFile.delete();
        indexFile.delete();
        metadataFile.delete();
    }


}
