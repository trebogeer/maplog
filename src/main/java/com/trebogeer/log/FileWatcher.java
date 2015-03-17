package com.trebogeer.log;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

/**
 * @author dimav
 *         Date: 3/17/15
 *         Time: 2:11 PM
 */
public class FileWatcher implements Runnable {

    private final FileSegment segment;

    private final Path path;

    public FileWatcher(FileSegment segment, Path path) {
        this.segment = segment;
        this.path = path;
    }

    @Override
    public void run() {
        try {
            WatchService watchService = path.getFileSystem().newWatchService();
            path.register(watchService,
                    StandardWatchEventKinds.ENTRY_MODIFY);

            // loop forever to watch directory
            while (true) {
                WatchKey watchKey;
                watchKey = watchService.take(); // this call is blocking until events are present

                // poll for file system events on the WatchKey
                for (final WatchEvent<?> event : watchKey.pollEvents()) {
                    // printEvent(event);
                }

                // if the watched directed gets deleted, get out of run method
                if (!watchKey.reset()) {
                    System.out.println("No longer valid");
                    watchKey.cancel();
                    watchService.close();
                    break;
                }
            }

        } catch (InterruptedException ex) {
            System.out.println("interrupted. Goodbye");
            return;
        } catch (IOException ex) {
            ex.printStackTrace();  // don't do this in production code. Use a loggin framework
            return;
        }
    }
}
