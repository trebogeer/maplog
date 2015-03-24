package com.trebogeer.maplog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 12:31 PM
 */
public class FileLog extends AbstractLog {

    private static final Logger logger = LoggerFactory.getLogger(FileLog.class);

    final FileLogConfig config;
    final File base;
    File partition = null;

    public FileLog(String name, FileLogConfig config) {
        super(config);
        this.config = config.copy();
        this.base = new File(config.getDirectory(), name);
        this.name = this.base.getAbsolutePath();
    }

    /**
     * Works only on linux. Retrieving monunts, resolving symlinks, trying to match by path segments.
     */
    private void initPartitionInfo() {
        if (Utils.isLinux()) {
            File info = new File("/proc/mounts");
            if (info.exists()) {
                try (FileReader fr = new FileReader(info); BufferedReader br = new BufferedReader(fr)) {
                    String s;
                    TreeSet<String> mounts = new TreeSet<>((o1, o2) -> o1.length() > o2.length() ? -1 : 1);
                    while ((s = br.readLine()) != null) {
                        s = s.split(" ")[1];
                        if (s != null) {
                            mounts.add(s);
                        }
                    }
                    for (String m : mounts) {
                        if (base.getCanonicalPath().startsWith(m)) {
                            partition = new File(m);
                            break;
                        }
                    }
                } catch (IOException e) {
                    logger.error("Error reading /proc/mounts.", e);
                }
            }
        }
    }

    @Override
    protected Collection<Segment> loadSegments() {
        Map<Short, Segment> segments = new HashMap<>();
        base.getAbsoluteFile().getParentFile().mkdirs();
        logger.info("Logging data to {}.", name);
        if (partition != null) {
            logger.info("Space available {} GB", partition.getUsableSpace() / 1024 / 1024 / 1024d);
        }
        for (File file : config.getDirectory().listFiles(File::isFile)) {
            if (file.getName().startsWith(base.getName() + "-") && file.getName().endsWith(".metadata")) {
                String st = file.getName().substring(file.getName().lastIndexOf('-') + 1);
                short id = Short.valueOf(st.substring(0, st.lastIndexOf('.')));
                if (!segments.containsKey(id)) {
                    segments.put(id, new File0LogSegment(this, id));
                }
            }
        }
        return segments.values();
    }

    @Override
    protected Segment createSegment(short segmentId) {
       return new File0LogSegment(this, segmentId);
    }

    @Override
    protected boolean checkSpaceAvailable() {
        if (partition != null) {
            long usable = partition.getUsableSpace();
            return usable > 0 &&
                    usable / (double) partition.getTotalSpace() > config.getStopWritesAtPercent() / 100f;
        }
        return true;
    }

    @Override
    public synchronized void open() throws IOException {
        initPartitionInfo();
        super.open();
        final FileLog f = this;
      /*  Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    f.close();
                } catch (IOException e) {
                    logger.error("Failed to close log file on jvm shutdown.", e);
                }
            }
        });*/
    }

}
