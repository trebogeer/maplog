package com.trebogeer.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:31 PM
 */
public class FileLog extends AbstractLog {

    private static final Logger logger = LoggerFactory.getLogger(FileLog.class);

    final FileLogConfig config;
    final File base;

    FileLog(String name, FileLogConfig config) {
        super(config);
        this.config = config.copy();
        this.base = new File(config.getDirectory(), name);
    }

    @Override
    protected Collection<Segment> loadSegments() {
        Map<Long, Segment> segments = new HashMap<>();
        base.getAbsoluteFile().getParentFile().mkdirs();
        logger.info("Logging data to [%s]", base.getAbsolutePath());
        for (File file : config.getDirectory().listFiles(File::isFile)) {
            if (file.getName().startsWith(base.getName() + "-") && file.getName().endsWith(".metadata")) {
                try {
                    String st = file.getName().substring(file.getName().lastIndexOf('-') + 1);
                    long id = Long.valueOf(st.substring(0, st.lastIndexOf('.')));
                    if (!segments.containsKey(id)) {
                        // Open the metadata file, determine the segment's first index, and create a log segment.
                        try (RandomAccessFile metaFile = new RandomAccessFile(file, "r")) {
                            long firstIndex = metaFile.readLong();
                            segments.put(id, new FileSegment(this, id));
                        }
                    }
                } catch (IOException | NumberFormatException e) {
                    throw new LogException("Error loading segments", e);
                }
            }
        }
        return segments.values();
    }

    @Override
    protected Segment createSegment(long segmentId, long firstIndex) {
        return new FileSegment(this, segmentId);
    }

}
