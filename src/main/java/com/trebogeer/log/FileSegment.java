package com.trebogeer.log;

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
public class FileSegment extends AbstractSegment {


    private final FileLog log;
    private final File logFile;
    private final File indexFile;
    private final File metadataFile;
    private long timestamp;
    private FileChannel logFileChannel;
    private FileChannel indexFileChannel;
    // private Long firstIndex;
    // private Long lastIndex;
    private boolean isEmpty = true;
    private long index;
    private final ByteBuffer indexBuffer = ByteBuffer.allocate(8);

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
        indexFileChannel.position(indexFileChannel.size());

        if (indexFileChannel.size() > 0) {
//            firstIndex = super.firstIndex;
//            lastIndex = firstIndex + indexFileChannel.size() / 8 - 1;
            isEmpty = false;
        }
    }

    @Override
    public boolean isEmpty() {
        assertIsOpen();
        // return firstIndex == null;
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
            throw new LogException("error opening file channel", e);
        }
    }


    @Override
    public byte[] appendEntry(ByteBuffer entry, byte[] index) {
        assertIsOpen();
        try {
            entry.rewind();
            long position = logFileChannel.position();
            logFileChannel.write(entry);
            storePosition(index, position);
            isEmpty = false;
        } catch (IOException e) {
            throw new LogException("error appending entry", e);
        }
        return index;
    }

    /**
     * Stores the position of an entry in the log.
     */
    private void storePosition(byte[] index, long position) {
//        try {
//            ByteBuffer buffer = ByteBuffer.allocate(8).putLong(position);
//            buffer.flip();
//            indexFileChannel.write(buffer, (index - firstIndex) * 8);
//        } catch (IOException e) {
//            throw new LogException("error storing position", e);
//        }
    }


    /**
     * Finds the position of the given index in the segment.
     */
    private long findPosition(long index) {
//        try {
//            if (firstIndex == null || index <= firstIndex) {
//                return 0;
//            } else if (lastIndex == null || index > lastIndex) {
//                return logFileChannel.size();
//            }
//            indexFileChannel.read(indexBuffer, (index - firstIndex) * 8);
//            indexBuffer.flip();
//            long position = indexBuffer.getLong();
//            indexBuffer.clear();
//            return position;
//        } catch (IOException e) {
//            throw new LogException("error finding position", e);
//        }
        return 0l;
    }


    @Override
    public ByteBuffer getEntry(byte[] index) {
        assertIsOpen();
        return null;
//        assertContainsIndex(index);
//        try {
//            long startPosition = findPosition(index);
//            long endPosition = findPosition(index + 1);
//            ByteBuffer buffer = ByteBuffer.allocate((int) (endPosition - startPosition));
//            logFileChannel.read(buffer, startPosition);
//            buffer.flip();
//            return buffer;
//        } catch (IOException e) {
//            throw new LogException("error retrieving entry", e);
//        }
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


}
