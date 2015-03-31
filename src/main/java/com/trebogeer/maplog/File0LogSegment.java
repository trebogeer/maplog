package com.trebogeer.maplog;

import com.trebogeer.maplog.index.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.COPY_ATTRIBUTES;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * @author dimav
 *         Date: 3/23/15
 *         Time: 2:32 PM
 */
public class File0LogSegment extends AbstractSegment {

    protected static final Logger logger = LoggerFactory.getLogger("JLOG.F.SEGMENT");

    private static final int INDEX_ENTRY_SIZE = 29;

    private final FileLog log;
    private final File logFile;
    private final File indexFile;
    private final File metadataFile;
    private long timestamp;
    protected FileChannel logReadFileChannel;
    protected FileChannel logWriteFileChannel;
    protected FileChannel indexReadFileChannel;
    protected FileChannel indexWriteFileChannel;
    protected FileChannel metaFileChannel;
    protected boolean isEmpty = true;
    protected ReentrantLock lock = new ReentrantLock();
    protected long lastCatchUpPosition = 0;
    protected String fullName;
    protected final ThreadLocal<ByteBuffer> indexBuffer = new ThreadLocal<ByteBuffer>() {

        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(INDEX_ENTRY_SIZE);
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
        this.fullName = log().name() + "/" + this.id;
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
    public void open() throws IOException {
        assertIsNotOpen();
        // TODO rethink to minimize IO
        metaFileChannel = FileChannel.open(this.metadataFile.toPath(), CREATE, READ, WRITE);
        FileLock mfl = null;
        try {
            ByteBuffer meta = ByteBuffer.allocate(10);
            if (metaFileChannel.size() == 0 && (mfl = metaFileChannel.tryLock(0, 16, false)) != null) {
                timestamp = System.nanoTime();
                meta.putShort(super.id);
                meta.putLong(timestamp);
                metaFileChannel.write(meta);
            } else {
                // TODO need to lock here
                metaFileChannel.read(meta);
                short metaFileIndex = meta.getShort();
                if (metaFileIndex != super.id) {
                    throw new LogException("Segment metadata out of sync " + fullName);
                }
                timestamp = meta.getLong();
            }
        } finally {
            if (mfl != null) {
                mfl.close();
            }
        }
        // write channel in append only mode

        logWriteFileChannel = FileChannel.open(this.logFile.toPath(), CREATE, APPEND);
        logReadFileChannel = FileChannel.open(this.logFile.toPath(), READ);

        logWriteFileChannel.position(logWriteFileChannel.size());
        logReadFileChannel.position(logReadFileChannel.size());

        indexWriteFileChannel = FileChannel.open(this.indexFile.toPath(), CREATE, APPEND);
        indexReadFileChannel = FileChannel.open(this.indexFile.toPath(), READ);

        long indexFileSize = indexReadFileChannel.size();
        if (indexFileSize != 0) {
            long start = System.currentTimeMillis();
            ByteBuffer b = indexBuffer.get();

            HashMap<Long, Value> localIndex = new HashMap<>();
            while (indexReadFileChannel.read(b) != -1) {
                b.flip();
                localIndex.put(b.getLong(), new Value(b.getLong(), b.getInt(), id(), b.get()));
                b.rewind();
            }
            log().index().putAll(localIndex);
            logger.info("Loaded segment {} index in memory. Elapsed time millis : {}, total number of segment entries: {}",
                    fullName, System.currentTimeMillis() - start, localIndex.size());
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
            throw new LogException("Error retrieving size from file channel seg: " + fullName, e);
        }
    }


    @Override
    // TODO 1. see if it would be easy/possible to push other pending changes under the same file lock.
    // TODO 2. see if it would make sense to try to lock only pos + data size region and let others do their thing.
    // TODO or maybe just expose bulk api or all of them
    public byte[] appendEntry(ByteBuffer entry, byte[] index, byte flags) {
        assertIsOpen();
        try {
            entry.rewind();
            lock.lock();
            int size = logWriteFileChannel.write(entry);
            storePosition(index, logWriteFileChannel.position() - size, size, flags);
            isEmpty = false;
        } catch (IOException e) {
            throw new LogException("Error appending entry seg: " + fullName, e);
        } finally {
            lock.unlock();
        }
        return index;
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
    public ByteBuffer getEntry(long position, int offset) {
        assertIsOpen();
        try {
            ByteBuffer buffer = ByteBuffer.allocate(offset /*- 4*/);
            logReadFileChannel.read(buffer, position /*+ 4*/);
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            throw new LogException("Error retrieving entry seg: "
                    + fullName + "/" + position + "/" + offset, e);
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

        metaFileChannel.close();
        metaFileChannel = null;
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


    /**
     * Compacts segment.
     */
    @Override
    public void compact() {

        if (this.id() != log().segment().id()) {
            // TODO may be have a changes threshold to optimize at. No need to
            FileChannel newIndex = null;
            FileChannel newLog = null;

            Path indexPath = Paths.get(indexFile.getAbsolutePath() + ".compact");
            Path dataPath = Paths.get(logFile.getAbsolutePath() + ".compact");

            try (FileLock fl = metaFileChannel.tryLock()) {
                if (fl != null && log().lastSegment().id() > id()) {
                    long start = System.currentTimeMillis();
                    FileChannel index = FileChannel.open(indexFile.toPath(), READ);
                    FileChannel dataLog = FileChannel.open(logFile.toPath(), READ);

                  /* Map<Long, Value> segmentCurrentView = log().index().entrySet().stream()
                            .filter(e -> e.getValue().getSegmentId() == id)
                            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
                    Map<Long, Value> segmentDiskView = new HashMap<>();*/

                    ByteBuffer bb = indexBuffer.get();
                    while (index.read(bb) != -1) {
                        bb.rewind();
                        Long key = bb.getLong();
                        bb.mark();
                        Value ov = new Value(bb.getLong(), bb.getInt(), id(), bb.get());
                        boolean expired = System.currentTimeMillis() - bb.getLong() >= TimeUnit.DAYS.toMillis(10);
                        Value cv = log().index().get(key);

                        if (cv != null) {
                            short curSegId = cv.getSegmentId();
                            short vSegId = ov.getSegmentId();
                            if (curSegId < vSegId || ((curSegId == vSegId) && cv.getPosition() < ov.getPosition())) {
                                if (newIndex == null) {
                                    Files.deleteIfExists(indexPath);
                                    Files.deleteIfExists(dataPath);

                                    newIndex = FileChannel.open(indexPath, CREATE, WRITE);
                                    newLog = FileChannel.open(dataPath, CREATE, WRITE);
                                }

                                dataLog.transferTo(ov.getPosition(), ov.getOffset(), newLog);
                                long pos = dataLog.position() - ov.getOffset();
                                bb.reset();
                                bb.putLong(pos);
                                bb.rewind();
                                newIndex.write(bb);
                            } else if (cv.equals(ov) && expired) {
                                // TODO repoint to compacted entry version
                            }
                        } else {
                            // TODO need to figure out if it should get deleted during compaction.
                        }
                        bb.rewind();
                    }
                    if (newLog != null) {

                        long oldSize = dataLog.size();
                        long newSize = newLog.size();
                        logger.info("Compacted segment {}:  {} -> {} in {} millis.",
                                fullName, oldSize, newSize, (System.currentTimeMillis() - start));
                    }

                    if (newIndex != null && newLog != null) {
                        try {
                            Files.move(indexPath, indexFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING, COPY_ATTRIBUTES);
                        } catch (IOException e) {
                            logger.error("Failed to move/rename compacted " + indexPath + " to " + indexFile.toPath());
                            return;
                        }
                        try {
                            Files.move(indexPath, indexFile.toPath(), ATOMIC_MOVE, REPLACE_EXISTING, COPY_ATTRIBUTES);
                        } catch (IOException e) {
                            logger.error("Failed to move/rename compacted " + indexPath + " to " + indexFile.toPath());
                        }
                    }

                }
            } catch (IOException ioe) {
                logger.error(String.format("Failed to compact segment %s due to an error.", fullName), ioe);
            }

        }
    }

    @Override
    // TODO avoid reading own changes. Need to optimize.
    // TODO rework lastCatchUpPosition marker.
    public void catchUp() {
        long start = System.currentTimeMillis();
        try (FileChannel index = FileChannel.open(indexFile.toPath(), READ)) {
            if (log().segment().id() == id()) {
                index.position(lastCatchUpPosition);
            }
            ByteBuffer bb = indexBuffer.get();
            while (index.read(bb) != -1) {
                bb.flip();
                log().index().put(bb.getLong(), new Value(bb.getLong(), bb.getInt(), id(), bb.get()));
                bb.rewind();
            }
        } catch (IOException io) {
            logger.error(String.format("Failed to catch up on segment %d.", id()), io);
        }
        logger.info("Catched up on changes in segment {}. Elapsed time {} millis.",
                fullName, (System.currentTimeMillis() - start));
    }
}

