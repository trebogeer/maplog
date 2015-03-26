package com.trebogeer.maplog;

import com.sun.nio.file.SensitivityWatchEventModifier;
import com.trebogeer.maplog.fsws.PollingWatchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;

import static com.sun.nio.file.SensitivityWatchEventModifier.*;
import static java.nio.file.StandardWatchEventKinds.*;

/**
 * @author dimav
 *         Date: 3/17/15
 *         Time: 2:11 PM
 */
public class FileWatcher implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

    private final Segment segment;

    private final Path path;

    private volatile boolean stop = false;

    private final boolean fsSupport;

    public FileWatcher(Segment segment, Path path, boolean fsSupport) {
        this.segment = segment;
        this.path = path;
        this.fsSupport = fsSupport;
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
                pollingWatchService.register(this.path, events, HIGH);
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
                    for (final WatchEvent<?> event : watchKey.pollEvents()) {
                        String ename = event.kind().name();
                        logger.info(ename);
                        /*if (ename.equals(StandardWatchEventKinds.ENTRY_MODIFY.name())) {
                            logger.info(event.kind().name());
                            // logger.info(event.count() + "");
                            // logger.info(event.context() + "");
                        } else if (ename.equals(StandardWatchEventKinds.ENTRY_CREATE)) {

                        } else *//*DELETE EVENT*//* {
                            logger.debug(event.kind().name());
                        }*/
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
