package com.trebogeer.maplog.fsws;

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author dimav
 *         Date: 3/26/15
 *         Time: 9:47 AM
 */

// TODO modify to only poll changes for current segment or newly created.
public class PollingWatchService
        extends CustomAbstractWatchService {

    // map of registrations
    private final Map<Object, PollingWatchKey> map = new HashMap<>();
    // used to execute the periodic tasks that poll for changes
    private final ScheduledExecutorService scheduledExecutor;

    public PollingWatchService() {
        // TBD: Make the number of threads configurable
        scheduledExecutor = Executors
                .newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true);
                    return t;
                });
    }

    /**
     * Register the given file with this watch service
     */
    @Override
    public WatchKey register(final Path path,
                             WatchEvent.Kind[] events,
                             WatchEvent.Modifier... modifiers)
            throws IOException {
        // check events - CCE will be thrown if there are invalid elements
        if (events.length == 0) {
            throw new IllegalArgumentException("No events to register");
        }
        final Set<WatchEvent.Kind> eventSet =
                new HashSet<>(events.length);
        for (WatchEvent.Kind event : events) {
            // standard events
            if (event == StandardWatchEventKinds.ENTRY_CREATE
                    || event == StandardWatchEventKinds.ENTRY_MODIFY
                    || event == StandardWatchEventKinds.ENTRY_DELETE) {
                eventSet.add(event);
                continue;
            }

            // OVERFLOW is ignored
            if (event == StandardWatchEventKinds.OVERFLOW) {
                if (events.length == 1) {
                    throw new IllegalArgumentException("No events to register");
                }
                continue;
            }

            // null/unsupported
            if (event == null) {
                throw new NullPointerException("An element in event set is 'null'");
            }
            throw new UnsupportedOperationException(event.name());
        }

        // A modifier may be used to specify the sensitivity level
        CustomSensivityWatchEventModifier sensivity = CustomSensivityWatchEventModifier.MEDIUM;
        if (modifiers.length > 0) {
            for (WatchEvent.Modifier modifier : modifiers) {
                if (modifier == null) {
                    throw new NullPointerException();
                }
                if (modifier instanceof CustomSensivityWatchEventModifier) {
                    sensivity = (CustomSensivityWatchEventModifier) modifier;
                    continue;
                }
                throw new UnsupportedOperationException("Modifier not supported");
            }
        }

        // check if watch service is closed
        if (!isOpen()) {
            throw new ClosedWatchServiceException();
        }

        // registration is done in privileged block as it requires the
        // attributes of the entries in the directory.
        try {
            final CustomSensivityWatchEventModifier s = sensivity;
            return (WatchKey) AccessController.doPrivileged(
                    (PrivilegedExceptionAction) () -> doPrivilegedRegister(path, eventSet,
                            s.sensitivityValueInMilliseconds()));
        } catch (PrivilegedActionException pae) {
            Throwable cause = pae.getCause();
            if (cause != null && cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new AssertionError(pae);
        }
    }

    // registers directory returning a new key if not already registered or
    // existing key if already registered
    private PollingWatchKey doPrivilegedRegister(Path path,
                                                 Set events,
                                                 int millisInterval)
            throws IOException {
        // check file is a directory and get its file key if possible
        BasicFileAttributes attrs = Files.readAttributes(path, BasicFileAttributes.class);
        if (!attrs.isDirectory()) {
            throw new NotDirectoryException(path.toString());
        }
        Object fileKey = attrs.fileKey();
        if (fileKey == null) {
            throw new AssertionError("File keys must be supported");
        }

        // grab close lock to ensure that watch service cannot be closed
        synchronized (closeLock()) {
            if (!isOpen()) {
                throw new ClosedWatchServiceException();
            }

            PollingWatchKey watchKey;
            synchronized (map) {
                watchKey = map.get(fileKey);
                if (watchKey == null) {
                    // new registration
                    watchKey = new PollingWatchKey(path, this, fileKey);
                    map.put(fileKey, watchKey);
                } else {
                    // update to existing registration
                    watchKey.disable();
                }
            }
            watchKey.enable(events, millisInterval);
            return watchKey;
        }

    }

    @Override
    void implClose() throws IOException {
        synchronized (map) {
            for (Map.Entry<Object, PollingWatchKey> entry : map.entrySet()) {
                PollingWatchKey watchKey = entry.getValue();
                watchKey.disable();
                watchKey.invalidate();
            }
            map.clear();
        }
        AccessController.doPrivileged((PrivilegedAction) () -> {
            scheduledExecutor.shutdown();
            return null;
        });
    }

    /**
     * Entry in directory cache to record file last-modified-time and tick-count
     */
    private static class CacheEntry {

        private long lastModified;
        private int lastTickCount;

        CacheEntry(long lastModified, int lastTickCount) {
            this.lastModified = lastModified;
            this.lastTickCount = lastTickCount;
        }

        int lastTickCount() {
            return lastTickCount;
        }

        void update(long lastModified, int tickCount) {
            this.lastModified = lastModified;
            this.lastTickCount = tickCount;
        }
    }

    /**
     * WatchKey implementation that encapsulates a map of the entries of the
     * entries in the directory. Polling the key causes it to re-scan the
     * directory and queue keys when entries are added, modified, or deleted.
     */
    private class PollingWatchKey extends CustomAbstractWatchKey {

        private final Object fileKey;
        // current event set
        private Set events;
        // the result of the periodic task that causes this key to be polled
        private ScheduledFuture poller;
        // indicates if the key is valid
        private volatile boolean valid;
        // used to detect files that have been deleted
        private int tickCount;
        // map of entries in directory
        private Map<Path, CacheEntry> entries;

        PollingWatchKey(Path dir, PollingWatchService watcher, Object fileKey)
                throws IOException {
            super(dir, watcher);
            this.fileKey = fileKey;
            this.valid = true;
            this.tickCount = 0;
            this.entries = new HashMap<>();

            // get the initial entries in the directory
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, (e) -> e.endsWith(".index"))) {
                for (Path entry : stream) {
                    // don't follow links
                    long lastModified =
                            Files.getLastModifiedTime(entry, LinkOption.NOFOLLOW_LINKS).toMillis();
                    entries.put(entry.getFileName(), new CacheEntry(lastModified, tickCount));
                }
            } catch (DirectoryIteratorException e) {
                throw e.getCause();
            }
        }

        Object fileKey() {
            return fileKey;
        }

        @Override
        public boolean isValid() {
            return valid;
        }

        void invalidate() {
            valid = false;
        }

        // enables periodic polling
        void enable(Set events, long period) {
            synchronized (this) {
                // update the events
                this.events = events;

                // create the periodic task
                Runnable thunk = this::poll;
                this.poller = scheduledExecutor
                        .scheduleAtFixedRate(thunk, period, period, TimeUnit.MICROSECONDS);
            }
        }

        // disables periodic polling
        void disable() {
            synchronized (this) {
                if (poller != null) {
                    poller.cancel(false);
                }
            }
        }

        @Override
        public void cancel() {
            valid = false;
            synchronized (map) {
                map.remove(fileKey());
            }
            disable();
        }

        /**
         * Polls the directory to detect for new files, modified files, or
         * deleted files.
         */
        synchronized void poll() {
            if (!valid) {
                return;
            }

            // update tick
            tickCount++;

            // open directory
            DirectoryStream<Path> stream;
            try {
                stream = Files.newDirectoryStream(watchable(), (e) -> e.endsWith(".index"));
            } catch (IOException x) {
                // directory is no longer accessible so cancel key
                cancel();
                signal();
                return;
            }

            // iterate over all entries in directory
            try {
                for (Path entry : stream) {
                    long lastModified;
                    try {
                        lastModified = Files.getLastModifiedTime(entry, LinkOption.NOFOLLOW_LINKS).toMillis();
                    } catch (IOException x) {
                        // unable to get attributes of entry. If file has just
                        // been deleted then we'll report it as deleted on the
                        // next poll
                        continue;
                    }

                    // lookup cache
                    CacheEntry e = entries.get(entry.getFileName());
                    if (e == null) {
                        // new file found
                        entries.put(entry.getFileName(),
                                new CacheEntry(lastModified, tickCount));

                        // queue ENTRY_CREATE if event enabled
                        if (events.contains(StandardWatchEventKinds.ENTRY_CREATE)) {
                            signalEvent(StandardWatchEventKinds.ENTRY_CREATE, entry.getFileName());
                            continue;
                        } else {
                            // if ENTRY_CREATE is not enabled and ENTRY_MODIFY is
                            // enabled then queue event to avoid missing out on
                            // modifications to the file immediately after it is
                            // created.
                            if (events.contains(StandardWatchEventKinds.ENTRY_MODIFY)) {
                                signalEvent(StandardWatchEventKinds.ENTRY_MODIFY, entry.getFileName());
                            }
                        }
                        continue;
                    }

                    // check if file has changed
                    if (e.lastModified != lastModified) {
                        if (events.contains(StandardWatchEventKinds.ENTRY_MODIFY)) {
                            signalEvent(StandardWatchEventKinds.ENTRY_MODIFY,
                                    entry.getFileName());
                        }
                    }
                    // entry in cache so update poll time
                    e.update(lastModified, tickCount);

                }
            } catch (DirectoryIteratorException e) {
                // ignore for now; if the directory is no longer accessible
                // then the key will be cancelled on the next poll
            } finally {

                // close directory stream
                try {
                    stream.close();
                } catch (IOException x) {
                    // ignore
                }
            }

            // iterate over cache to detect entries that have been deleted
            Iterator<Map.Entry<Path, CacheEntry>> i = entries.entrySet().iterator();
            while (i.hasNext()) {
                Map.Entry<Path, CacheEntry> mapEntry = i.next();
                CacheEntry entry = mapEntry.getValue();
                if (entry.lastTickCount() != tickCount) {
                    Path name = mapEntry.getKey();
                    // remove from map and queue delete event (if enabled)
                    i.remove();
                    if (events.contains(StandardWatchEventKinds.ENTRY_DELETE)) {
                        signalEvent(StandardWatchEventKinds.ENTRY_DELETE, name);
                    }
                }
            }
        }
    }
}