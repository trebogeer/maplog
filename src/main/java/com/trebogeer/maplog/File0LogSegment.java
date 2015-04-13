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
    protected FileChannel logReadFileChannel;
    protected FileChannel logWriteFileChannel;
    protected FileChannel indexReadFileChannel;
    protected FileChannel indexWriteFileChannel;
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

    File0LogSegment(FileLog log, short id) {
        super(id);
        this.log = log;
        this.logFile = new File(log.base.getParentFile(), String.format("%s-%d.log", log.base.getName(), id));
        this.indexFile = new File(log.base.getParentFile(), String.format("%s-%d.index", log.base.getName(), id));
        this.fullName = log().name() + "/" + this.id;
    }

    @Override
    public Log<Long> log() {
        return log;
    }

    @Override
    public synchronized void open() throws IOException {
        if (isOpen()) return;
        // TODO rethink to minimize IO
        // write channel in append only mode
        logWriteFileChannel = FileChannel.open(this.logFile.toPath(), CREATE, APPEND);
        logReadFileChannel = FileChannel.open(this.logFile.toPath(), READ);

        long dataLogSize;
        logWriteFileChannel.position((dataLogSize = logWriteFileChannel.size()));

        indexWriteFileChannel = FileChannel.open(this.indexFile.toPath(), CREATE, APPEND);
        indexReadFileChannel = FileChannel.open(this.indexFile.toPath(), READ);

        long indexFileSize = indexReadFileChannel.size();
        // TODO read everything into buffer then loop?
        if (indexFileSize != 0) {
            long start = System.currentTimeMillis();
            ByteBuffer b = indexBuffer.get();

            HashMap<Long, Value> localIndex = new HashMap<>();
            while (indexReadFileChannel.read(b) != -1) {
                b.flip();
                try {
                    localIndex.put(b.getLong(), new Value(b.getLong(), b.getInt(), id(), b.get()));
                } catch (BufferUnderflowException e) {
                    logger.warn("Partial index entry found at {}.", fullName);
                }
                b.rewind();
            }
            log().index().putAll(localIndex);
            logger.info("Loaded segment {} index in memory. Elapsed time millis : {}, total number of segment entries: {}",
                    fullName, System.currentTimeMillis() - start, localIndex.size());
        }
        maxSeenPosition.set(dataLogSize);
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
    public byte[] appendEntry(ByteBuffer entry, byte[] index, byte flags) {
        assertIsOpen();
        try {
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
            throw new LogException("Error appending entry seg: " + fullName, e);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } finally {
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
    public void closeWrite() throws IOException {
        /*new Thread(() -> {
            try {
                lock.lockInterruptibly();
                if (logWriteFileChannel != null) {
                    logWriteFileChannel.close();
                    logWriteFileChannel = null;
                    indexWriteFileChannel.close();
                    indexWriteFileChannel = null;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                lock.unlock();
            }
        }).start();*/
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

        if (this.id() != log().segment().id() && indexWriteFileChannel == null) {
            try (FileChannel indexWriteFileChannel = FileChannel.open(indexFile.toPath(), WRITE)) {
                lock.lockInterruptibly();
                FileLock fl = indexWriteFileChannel.tryLock();
                if (fl != null) {
                    long start = System.currentTimeMillis();
                    FileChannel index = indexReadFileChannel;
                    FileChannel dataLog = logReadFileChannel;
                    long oldSize = dataLog.size();
                    try (
                            FileChannel logWriteFileChannel = FileChannel.open(logFile.toPath(), WRITE)) {
                        ByteBuffer ibb = indexBuffer.get();
                        ByteBuffer bb = ByteBuffer.allocate((int) index.size());
                        long bytesRead = index.read(bb);
                        long pos = 0;
                        if (bytesRead > 0) {
                            while (bb.hasRemaining()) {
                                try {
                                    Long key = bb.getLong();
                                    Value ov = new Value(bb.getLong(), bb.getInt(), id(), bb.get());
                                    long ts = bb.getLong();
                                    boolean expired = System.currentTimeMillis() - bb.getLong() >= TimeUnit.DAYS.toMillis(10);
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

                                    } else if (ov.equals(cv) && expired) {
                                        // TODO compact expired
                                    }
                                } catch (BufferUnderflowException bue) {
                                    logger.warn("Partial index entry found at {}.", fullName);
                                }
                            }
                            logWriteFileChannel.truncate(pos);
                            flush();
                        }
                        logger.info("Compacted segment {} in {} millis. {} -> {}", fullName, System.currentTimeMillis() - start, oldSize, pos);
                    }

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

 /*               while (bb.limit() < INDEX_ENTRY_SIZE
                        && retryAttempts < CATCH_UP_RETRIES) {
                    logger.debug("Index entry buffer if not fully read. Bytes read: {}, bytes expected {}"
                            , bb.limit(), INDEX_ENTRY_SIZE);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    retryAttempts++;
                    if (retryAttempts > CATCH_UP_RETRIES) {
                        logger.error("Failed to retrieve index entry (read: {}, expected: {})" +
                                        " and exceeded maximum number ({}) of retry attempts. Aborting catchup." +
                                        " Either index is corrupted or storage is not accessible.",
                                bb.limit(), INDEX_ENTRY_SIZE, CATCH_UP_RETRIES);
                        return;
                    }
                }*/

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

}

