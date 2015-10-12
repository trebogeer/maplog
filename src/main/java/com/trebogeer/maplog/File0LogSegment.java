package com.trebogeer.maplog;

import com.trebogeer.maplog.index.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static com.trebogeer.maplog.Constants.CRC_AND_SIZE;
import static com.trebogeer.maplog.Constants.INDEX_ENTRY_SIZE;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.stream.Collectors.toCollection;

/**
 * @author dimav
 *         Date: 3/23/15
 *         Time: 2:32 PM
 */

public class File0LogSegment extends AbstractSegment {

    protected static final Logger logger = LoggerFactory.getLogger("JLOG.F.SEGMENT");

    protected final FileLog log;
    private final File logFile;
    private final File indexFile;
    protected volatile FileChannel logReadFileChannel;
    protected volatile FileChannel logWriteFileChannel;
    protected volatile FileChannel indexReadFileChannel;
    protected volatile FileChannel indexWriteFileChannel;
    protected boolean isEmpty = true;
    protected ReentrantLock lock = new ReentrantLock();
    protected ReentrantLock readRepairLock = new ReentrantLock();
    protected Condition readRepairSuccess = readRepairLock.newCondition();
    protected long lastCatchUpPosition = 0;
    protected String fullName;
    protected AtomicLong maxSeenPosition = new AtomicLong(0);

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

    protected final ThreadLocal<ByteBuffer> crc_and_size = new ThreadLocal<ByteBuffer>() {

        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocate(CRC_AND_SIZE);
        }

        @Override
        public ByteBuffer get() {
            ByteBuffer bb = super.get();
            bb.rewind();
            return bb;
        }
    };

    File0LogSegment(FileLog log, int id) {
        super(id);
        this.log = log;
        this.logFile = new File(log.base.getParentFile(), String.format("%s-%d.log", log.base.getName(), id));
        this.indexFile = new File(log.base.getParentFile(), String.format("%s-%d.index", log.base.getName(), id));
        this.fullName = log().name() + "/" + this.id;
    }


    File0LogSegment(FileLog log, int id, Function<ByteBuffer, ByteBuffer> compactor) {
        this(log, id);
        this.compactor = compactor;
    }

    @Override
    public Log<Long> log() {
        return log;
    }

    protected void openWriteChannels() throws IOException {
        if (logWriteFileChannel == null) {
            synchronized (this) {
                if (logWriteFileChannel == null)
                    logWriteFileChannel = FileChannel.open(this.logFile.toPath(), CREATE, APPEND);
            }
        }
        if (indexWriteFileChannel == null) {
            synchronized (this) {
                if (indexWriteFileChannel == null)
                    indexWriteFileChannel = FileChannel.open(this.indexFile.toPath(), CREATE, APPEND);
            }
        }
    }

    @Override
    // TODO rethink to minimize IO. See if open-close write channels can be avoided. Address race condition when file gets deleted during compaction but gets restored upon open.
    // TODO it's also possible to open previously deleted file and start writing to it.
    public synchronized void open() throws IOException {
        try {
            lock.lock();
            if (isOpen()) return;
            openWriteChannels();
            // closeWriteChannels();
            logReadFileChannel = FileChannel.open(this.logFile.toPath(), READ);
            indexReadFileChannel = FileChannel.open(this.indexFile.toPath(), READ);

            long indexFileSize = indexReadFileChannel.size();

            long lastSeenPos = 0;
            if (indexFileSize != 0) {
                ByteBuffer b = ByteBuffer.allocate((int) indexFileSize);
                long start = System.currentTimeMillis();
                HashMap<Long, Value> localIndex = new HashMap<>();
                indexReadFileChannel.read(b);
                b.rewind();
                while (b.hasRemaining()) {
                    try {
                        localIndex.put(b.getLong(), new Value(lastSeenPos = b.getLong(), b.getInt(), id(), b.get()));
                        b.getLong();
                    } catch (BufferUnderflowException e) {
                        logger.warn("Partial index entry found at {}.", fullName);
                    }

                }
                log().index().putAll(localIndex);
                logger.info("Loaded segment {} index in memory. Elapsed time millis : {}, total number of segment entries: {}",
                        fullName, System.currentTimeMillis() - start, localIndex.size());
            }
            maxSeenPosition.set(lastSeenPos);
            if (indexFileSize > 0) {
                isEmpty = false;
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        assertIsOpen();
        return isEmpty;
    }

    @Override
    public boolean isOpen() {
        return logReadFileChannel != null && indexReadFileChannel != null;
    }

    @Override
    public long size() {
        assertIsOpen();
        return maxSeenPosition.get();
    }


    @Override
    // TODO 1. see if it would be easy/possible to push other pending changes under the same file lock.
    // TODO 2. see if it would make sense to try to lock only pos + data size region and let others do their thing.
    // TODO or maybe just expose bulk api or all of them
    // TODO try lock with timeout - sort of back pressure if abused
    // TODO add exception as per interface signature.
    public byte[] appendEntry(ByteBuffer entry, byte[] index, byte flags) {
        try {
            openWriteChannels();
            entry.rewind();
            int checksum = log.checksum().checksum(entry);
            long s = entry.limit() - entry.position();
            ByteBuffer crcAndSize = crc_and_size.get();
            crcAndSize.putLong(s).putInt(checksum);
            crcAndSize.rewind();
            lock.lockInterruptibly();
            // TODO casting for now, will need better handling
            int size = (int) logWriteFileChannel.write(new ByteBuffer[]{crcAndSize, entry});
            long pos = logWriteFileChannel.position();
            maxSeenPosition.set(pos);
            storePosition(index, pos - size, size, flags);
            isEmpty = false;
            flush();
        } catch (IOException e) {
            logger.error("Error appending entry seg: " + fullName, e);
            throw new LogException("Error appending entry seg: " + fullName, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
            if (lock.isHeldByCurrentThread())
                lock.unlock();
        }
        return index;
    }

    /**
     * Appends an entry to the logger with specified id.
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
        return entries.entrySet().stream()
                .filter(entry -> (entry != null) && (entry.getValue() != null))
                .map(entry -> appendEntry(entry.getValue().getEntry(), entry.getKey(), entry.getValue().getMeta()))
                .collect(toCollection(LinkedList::new));
    }

    /**
     * Stores the position of an entry in the log.
     */

    protected int storePosition(byte[] index, long position, int offset, byte flags) {

        try {
            long key = log.hash.hash(index);
            ByteBuffer buffer = indexBuffer.get().
                    putLong(key).putLong(position).putInt(offset).put(flags).putLong(System.nanoTime());
            buffer.flip();
            int size = indexWriteFileChannel.write(buffer);
            log().index().merge(key, new Value(position, offset, id(), flags),
                    (v0, v1) -> v0.getSegmentId() < v1.getSegmentId()
                            || (v0.getSegmentId() == v0.getSegmentId()
                            && v0.getPosition() < v1.getPosition())
                            ? v1 : v0);
            return size;
        } catch (IOException e) {
            throw new LogException("Error storing position seg: "
                    + fullName + "/" + position + "/" + offset, e);
        }
    }

    @Override
    public ByteBuffer getEntry(long key, Value v) throws IOException {
        if (v == null) return null;
        int offset = -1;
        long position = -1;
        assertIsOpen();
        try {
            offset = v.getOffset();
            position = v.getPosition();
            ByteBuffer buffer = ByteBuffer.allocate(offset);
            logReadFileChannel.read(buffer, position);
            buffer.rewind();
            long s = buffer.getLong() + CRC_AND_SIZE;
            int chs = buffer.getInt();
            if (s != offset || chs != log.checksum().checksum(buffer)) {
                if (!repair()) {
                    try {
                        // TODO make it configurable
                        readRepairSuccess.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                Value vv = log().index().get(key);
                if (vv == null) return null;
                if (offset != vv.getOffset()) {
                    buffer = ByteBuffer.allocate(offset = vv.getOffset());
                } else {
                    buffer.rewind();
                }
                logReadFileChannel.read(buffer, vv.getPosition());
                s = buffer.getLong() + CRC_AND_SIZE;
                chs = buffer.getInt();
                if (s != offset || chs != log.checksum().checksum(buffer)) {
                    logger.error("Corrupted entry - seg: {}, position - {}, offset - {}",
                            fullName, position, offset);
                    return null;
                }
            }
            // buffer.rewind();
            return buffer;
        } catch (IOException e) {
            throw new IOException("Error retrieving entry seg: "
                    + fullName + "/" + position + "/" + offset, e);
        }
    }

    protected boolean repair() {
        try {

            boolean locked = readRepairLock.tryLock();
            if (locked) {
                long start = System.currentTimeMillis();
                long size = indexReadFileChannel.size();

                if (size > 0) {
                    //logger.info("Repairing segment {} index.", fullName);
                    final ByteBuffer b = ByteBuffer.allocate((int) size);
                    indexReadFileChannel.read(b);
                    while (b.hasRemaining()) {
                        try {
                            log().index().merge(b.getLong(), new Value(b.getLong(), b.getInt(), id(), b.get()),
                                    (v0, v1) -> v0.getSegmentId() < v1.getSegmentId()
                                            || (v0.getSegmentId() == v0.getSegmentId()
                                            && v0.getPosition() < v1.getPosition())
                                            ? v1 : v0);
                            b.getLong();
                        } catch (BufferUnderflowException bue) {
                            logger.warn("Partial index entry found at {}.", fullName);
                        }
                    }

                    logger.info("Repaired segment {} index in {} millis.",
                            fullName, (System.currentTimeMillis() - start));
                }
                readRepairSuccess.signalAll();
                return true;
            }

        } catch (IOException e) {
            logger.error(String.format("Read repair of segment %s failed", fullName), e);
        } finally {
            readRepairLock.unlock();
        }
        return false;
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
    // TODO close when all writes complete.
    public synchronized void close() throws IOException {
        if (isClosed()) return;
        logger.info("Closing segment [{}]", id());
        if (logWriteFileChannel != null) {
            logWriteFileChannel.close();
            logWriteFileChannel = null;
            indexWriteFileChannel.close();
            indexWriteFileChannel = null;
        }

        logReadFileChannel.close();
        logReadFileChannel = null;
        indexReadFileChannel.close();
        indexReadFileChannel = null;
    }

    @Override
    public void closeWrite() throws IOException {
        try {
            lock.lockInterruptibly();
            closeWriteChannels();
            logger.info("Closed write channels for segment {}", fullName);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lock.unlock();
        }
    }

    private void closeWriteChannels() throws IOException {
        if (logWriteFileChannel != null) {
            synchronized (this) {
                if (logWriteFileChannel != null) {
                    logWriteFileChannel.close();
                    logWriteFileChannel = null;
                }
            }
        }
        if (indexWriteFileChannel != null) {
            synchronized (this) {
                if (indexWriteFileChannel != null) {
                    indexWriteFileChannel.close();
                    indexWriteFileChannel = null;
                }
            }
        }
    }

    @Override
    public boolean isClosed() {
        return logReadFileChannel == null;
    }

    @Override
    public void delete() {
        logFile.delete();
        indexFile.delete();
    }


    /**
     * Compacts segment.
     */
    @Override
    // TODO may be have a changes threshold to optimize at.
    // TODO try compacting on low space left too.
    public void compact() {

        if (this.id() != log().segment().id() /*&& indexWriteFileChannel == null*/) {
            try (FileChannel indexWriteFileChannel = FileChannel.open(indexFile.toPath(), WRITE)) {
                if (lock.tryLock(5, TimeUnit.SECONDS)) {
                    // will get released on close, so not closing it explicitly
                    FileLock fl = indexWriteFileChannel.tryLock();
                    if (fl != null) {

                        long start = System.currentTimeMillis();
                        FileChannel index = indexReadFileChannel;
                        FileChannel dataLog = logReadFileChannel;
                        long oldSize = dataLog.size();
                        try (FileChannel logWriteFileChannel = FileChannel.open(logFile.toPath(), WRITE)) {
                            ByteBuffer ibb = indexBuffer.get();
                            int is = (int) index.size();
                            ByteBuffer bb = ByteBuffer.allocate(is);
                            index.read(bb);
                            long pos = 0;
                            long indexEntries = 0;
                            bb.rewind();
                            if (bb.limit() > 0) {
                                while (bb.hasRemaining()) {
                                    try {
                                        Long key = bb.getLong();
                                        Value ov = new Value(bb.getLong(), bb.getInt(), id(), bb.get());
                                        long ts = bb.getLong();
                                        boolean expired = System.currentTimeMillis() - ts >= TimeUnit.DAYS.toMillis(10);
                                        Value cv = log().index().get(key);

                                        if (cv == null || !(cv.getSegmentId() > ov.getSegmentId()
                                                || ((cv.getSegmentId() == ov.getSegmentId())
                                                && cv.getPosition() > ov.getPosition()))) {
                                            ibb.rewind();
                                            ibb.putLong(key);
                                            ibb.putLong(pos);
                                            ibb.putInt(ov.getOffset());
                                            ibb.put(ov.getFlags());
                                            ibb.putLong(ts);
                                            ibb.rewind();
                                            logWriteFileChannel.transferFrom(dataLog, ov.getPosition(), ov.getOffset());
                                            indexWriteFileChannel.write(ibb);
                                            pos += ov.getOffset();
                                            indexEntries++;

                                        } else if (ov.equals(cv) && expired) {

                                            ByteBuffer toCompact = ByteBuffer.allocate(ov.getOffset());
                                            dataLog.read(toCompact, ov.getPosition());
                                            ByteBuffer compacted = compactor.apply(toCompact);

                                            ibb.rewind();
                                            ibb.putLong(key);
                                            ibb.putLong(pos);
                                            ibb.putInt(compacted.limit());
                                            ibb.put(ov.getFlags());
                                            ibb.putLong(ts);
                                            ibb.rewind();

                                            logWriteFileChannel.write(compacted);
                                            indexWriteFileChannel.write(ibb);
                                            pos += compacted.limit();
                                            indexEntries++;
                                        }
                                    } catch (BufferUnderflowException bue) {
                                        logger.warn("Partial index entry found at {}.", fullName);
                                    }
                                }
                                if (pos > 0 && indexEntries > 0) {
                                    logWriteFileChannel.truncate(pos);
                                    logWriteFileChannel.force(false);
                                    indexWriteFileChannel.truncate(Constants.INDEX_ENTRY_SIZE * indexEntries);
                                    indexWriteFileChannel.force(false);
                                } else {
                                    close();
                                    delete();
                                    log.remove(id());
                                    logger.info("Deleted segment {}", fullName);
                                    return;
                                }
                            }
                            logger.info("Compacted segment {} in {} millis. {} -> {}", fullName, System.currentTimeMillis() - start, oldSize, pos);
                        }


                    } else {
                        logger.debug("Skipped compacting segment {}. Locked by other instance.", fullName);
                    }
                } else {
                    logger.debug("Skipped compacting segment {}. Locked by other thread.", fullName);
                }
            } catch (IOException ioe) {
                logger.error(String.format("Failed to compact segment %s due to an error.", fullName), ioe);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }

        }
    }

    @Override
    // TODO avoid reading own changes. Need to optimize.
    // TODO rework lastCatchUpPosition marker.
    // TODO need to reuse existing channel
    public void catchUp() {
        long start = System.currentTimeMillis();
        try (FileChannel index = FileChannel.open(indexFile.toPath(), READ)) {
            if (log().segment().id() == id()) {
                index.position(lastCatchUpPosition);
            }
            ByteBuffer bb = indexBuffer.get();
            int read;
            while ((read = index.read(bb)) != -1 && read == INDEX_ENTRY_SIZE) {
                bb.flip();
                try {
                    log().index().put(bb.getLong(), new Value(bb.getLong(), bb.getInt(), id(), bb.get()));
                } catch (BufferUnderflowException e) {
                    logger.error("Failed to retrieve index entry. Buffer - pos: {}, limit: {}, expected: {}",
                            bb.position(), bb.limit(), INDEX_ENTRY_SIZE);
                    return;
                }
                bb.rewind();
            }
            if (read != -1) {
                logger.debug("Catch up {} last buffer size: {}", fullName, read);
            }
            lastCatchUpPosition = index.position();
            maxSeenPosition.getAndUpdate((p) -> p > lastCatchUpPosition ? p : lastCatchUpPosition);
        } catch (IOException io) {
            logger.error(String.format("Failed to catch up on segment %d.", id()), io);
        }
        logger.info("Catched up on changes in segment {}. Elapsed time {} millis.",
                fullName, (System.currentTimeMillis() - start));
    }

    @Override
    public String toString() {
        return "File0LogSegment{" +
                "fullName='" + fullName + '\'' +
                '}';
    }
}

