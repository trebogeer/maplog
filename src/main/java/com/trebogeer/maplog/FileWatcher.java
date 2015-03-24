package com.trebogeer.maplog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author dimav
 *         Date: 3/17/15
 *         Time: 2:11 PM
 */
public class FileWatcher implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(FileWatcher.class);

    private final FileSegment segment;

    private final Path path;

    private AtomicBoolean stop = new AtomicBoolean(false);

    public FileWatcher(FileSegment segment, Path path) {
        this.segment = segment;
        this.path = path;
    }

    @Override
    public void run() {
        try {
            WatchService watchService = path.getFileSystem().newWatchService();
            path.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE);

            // loop forever to watch directory
            while (!stop.get()) {
                WatchKey watchKey;
                watchKey = watchService.take(); // this call is blocking until events are present

                // poll for file system events on the WatchKey
                for (final WatchEvent<?> event : watchKey.pollEvents()) {
                    String ename = event.kind().name();
                    if (ename.equals(StandardWatchEventKinds.ENTRY_MODIFY.name())) {
                       // logger.info(event.kind().name());
                       // logger.info(event.count() + "");
                       // logger.info(event.context() + "");
                    } else if (ename.equals(StandardWatchEventKinds.ENTRY_CREATE)) {

                    } else /*DELETE EVENT*/{
                        logger.debug(event.kind().name());
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

        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (IOException ex) {
            logger.error("Error watching directory for updates.", ex);
        }
    }

    public void shutdown() {
        this.stop.set(true);
    }
}
