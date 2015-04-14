package com.trebogeer.maplog;

import com.trebogeer.maplog.fsws.FileWatcher;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.trebogeer.maplog.Utils.fixedThreadNamingExecutorService;
import static com.trebogeer.maplog.Utils.isLinux;
import static com.trebogeer.maplog.Utils.shutdownExecutor;
import static java.io.File.separator;
import static java.lang.Thread.MIN_PRIORITY;
import static java.lang.Thread.NORM_PRIORITY;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

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
    final ExecutorService catchUp;
    final ExecutorService compact;
    final FileWatcher fileWatcher;
    final CompactionScheduler compactionScheduler;

    public FileLog(String name, FileLogConfig config) {
        super(config);
        this.config = config.copy();
        this.base = new File(config.getDirectory(), name);
        this.name = this.base.getAbsolutePath();
        this.catchUp = fixedThreadNamingExecutorService(1, "catch-up-thread-" + name().replaceAll(separator, "-"), NORM_PRIORITY);
        this.compact = fixedThreadNamingExecutorService(1, "compact-thread-" + name().replaceAll(separator, "-"), MIN_PRIORITY);
        this.fileWatcher = new FileWatcher(this, base.getAbsoluteFile().getParentFile().toPath(), true);
        // TODO move parameters to config
        this.compactionScheduler = new CompactionScheduler(this, 6, HOURS);
    }

    /**
     * Works only on linux. Retrieving mounts, resolving symlinks, trying to match by path segments.
     */
    private void initPartitionInfo() {
        // TODO switch to FileStore instead
        if (isLinux()) {
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
            if (file.getName().startsWith(base.getName() + "-") && file.getName().endsWith(".index")) {
                String st = file.getName().substring(file.getName().lastIndexOf('-') + 1);
                short id = Short.valueOf(st.substring(0, st.lastIndexOf('.')));
                if (!segments.containsKey(id)) {
                    segments.put(id, createSegment(id));
                }
            }
        }
        return segments.values();
    }

    @Override
    protected Segment createSegment(short segmentId) {
        return config.isLockFiles() ? new LockingLogSegment(this, segmentId) : new File0LogSegment(this, segmentId);
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
        catchUp.execute(fileWatcher);
        compact.execute(compactionScheduler);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    if (!f.isClosed()) {
                        f.close();
                    }
                } catch (IOException e) {
                    logger.error("Failed to close log file on jvm shutdown.", e);
                }
            }
        });
    }

    @Override
    public synchronized void close() throws IOException {
        fileWatcher.shutdown();
        compactionScheduler.shutdown();
        shutdownExecutor(2, TimeUnit.MINUTES, catchUp);
        shutdownExecutor(2, TimeUnit.MINUTES, compact);
        super.close();
    }

    private static class CompactionScheduler implements Runnable {

        private final FileLog fl;
        private final long interval;
        private final TimeUnit tu;
        private volatile boolean stop = false;
        private volatile boolean forceCompact = false;

        public CompactionScheduler(FileLog fl, long interval, TimeUnit tu) {
            this.fl = fl;
            this.interval = interval;
            this.tu = tu;
        }

        @Override
        public void run() {
            {
                try {

                    Thread.sleep(SECONDS.toMillis(30));

                    while (true) {
                        fl.compact();
                        forceCompact = false;
                        long nextRun = System.currentTimeMillis() + tu.toMillis(interval);
                        long time = System.currentTimeMillis();
                        while (time < nextRun) {

                            if (forceCompact) {
                                break;
                            } else if (stop) {
                                return;
                            } else {
                                Thread.sleep(SECONDS.toMillis(5));
                                time = System.currentTimeMillis();
                            }

                        }
                    }

                } catch (IOException io) {
                    logger.error("Compaction failed for log " + fl.name(), io);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public synchronized void shutdown() {
            this.stop = true;
        }

        public synchronized void force() {
            this.forceCompact = true;
        }
    }
}
