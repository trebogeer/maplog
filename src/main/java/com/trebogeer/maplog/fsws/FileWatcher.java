package com.trebogeer.maplog.fsws;

import com.trebogeer.maplog.FileLog;
import com.trebogeer.maplog.Segment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import static com.trebogeer.maplog.fsws.CustomSensivityWatchEventModifier.VERY_HIGH;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * @author dimav
 *         Date: 3/17/15
 *         Time: 2:11 PM
 */
public class FileWatcher implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

    private final Path path;

    private volatile boolean stop = false;

    private final boolean fsSupport;

    private final FileLog log;

    public FileWatcher(FileLog fl, Path path, boolean fsSupport) {
        this.path = path;
        this.fsSupport = fsSupport;
        this.log = fl;
    }

    @Override
    public void run() {
        try {
            WatchService watchService;
            if (fsSupport) {
                watchService = path.getFileSystem().newWatchService();
                path.register(watchService, ENTRY_MODIFY, ENTRY_CREATE, ENTRY_DELETE);
            } else {
                PollingWatchService pollingWatchService = new PollingWatchService();
                WatchEvent.Kind[] events = {ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY};
                pollingWatchService.register(this.path, events, VERY_HIGH);
                watchService = pollingWatchService;
            }
            // loop forever to watch directory
            while (!this.stop) {
                //  System.out.println(stop);
                WatchKey watchKey;
                // this call is blocking until events are present or timeout happens
                watchKey = watchService.poll(1000, TimeUnit.MILLISECONDS);
                if (watchKey != null) {
                    // poll for file system events on the WatchKey
                    // TODO collaps same events on same file
                    for (final WatchEvent<?> event : watchKey.pollEvents()) {
                        String ename = event.context().toString();
                        logger.info(ename);
                        if (ename.endsWith(".index")) {
                            try {
                                String st = ename.substring(ename.lastIndexOf('-') + 1);
                                short id = Short.valueOf(st.substring(0, st.lastIndexOf('.')));
                                if (event.kind().equals(ENTRY_MODIFY)) {

                                    Segment s = log.segment(id);
                                    if (s != null) {
                                        s.catchUp();
                                    }
                                } else if (event.kind().equals(ENTRY_CREATE)) {
                                    Segment s = log.segment(id);
                                    if (s == null) {
                                        log.rollOver();
                                        s = log.lastSegment();
                                    }
                                    if (s != null) {
                                        s.catchUp();
                                    } else {
                                        logger.error("Failed to rollover seg " + ename);
                                    }
                                }
                            } catch (NumberFormatException nfe) {
                                logger.error("Unknown index file " + ename, nfe);
                            }
                        } else if (ename.endsWith(".index.compact")) {
                            String st = ename.substring(ename.lastIndexOf('-') + 1);
                            short id = Short.valueOf(st.substring(0, st.lastIndexOf(".index.compact")));
                            // TODO open compacted file, change index pointers for already migrated values to compacted file

                        }
                    }

                    // if the watched directed gets deleted, get out of run method
                    if (!watchKey.reset()) {
                        logger.info("No longer valid.");
                        watchKey.cancel();
                        watchService.close();
                        break;
                    }
                }
            }

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (IOException ex) {
            logger.error("Error watching directory for updates.", ex);
        }
    }

    public synchronized void shutdown() {
        this.stop = true;
    }
}
