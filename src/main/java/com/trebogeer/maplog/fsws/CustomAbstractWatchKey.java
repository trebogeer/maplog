/*
 * Copyright (c) 2008, 2011, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
*/


package com.trebogeer.maplog.fsws;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base implementation class for watch services.
 */
public abstract class CustomAbstractWatchKey implements WatchKey {

    static final int MAX_EVENT_LIST_SIZE = 512;
    static final CustomAbstractWatchKey.Event<Object> OVERFLOW_EVENT;
    private final CustomAbstractWatchService watcher;
    private final Path dir;
    private CustomAbstractWatchKey.State state;
    private List<WatchEvent<?>> events;
    private Map<Object, WatchEvent<?>> lastModifyEvents;

    protected CustomAbstractWatchKey(Path var1, CustomAbstractWatchService var2) {
        this.watcher = var2;
        this.dir = var1;
        this.state = CustomAbstractWatchKey.State.READY;
        this.events = new ArrayList<>();
        this.lastModifyEvents = new HashMap<>();
    }

    final CustomAbstractWatchService watcher() {
        return this.watcher;
    }

    public Path watchable() {
        return this.dir;
    }

    final void signal() {
        synchronized (this) {
            if (this.state == CustomAbstractWatchKey.State.READY) {
                this.state = CustomAbstractWatchKey.State.SIGNALLED;
                this.watcher.enqueueKey(this);
            }

        }
    }

    final void signalEvent(WatchEvent.Kind<?> var1, Object var2) {
        boolean var3 = var1 == StandardWatchEventKinds.ENTRY_MODIFY;
        synchronized (this) {
            int var5 = this.events.size();
            if (var5 > 0) {
                WatchEvent var6 = (WatchEvent) this.events.get(var5 - 1);
                if (var6.kind() == StandardWatchEventKinds.OVERFLOW || var1 == var6.kind() && Objects.equals(var2, var6.context())) {
                    ((CustomAbstractWatchKey.Event) var6).increment();
                    return;
                }

                if (!this.lastModifyEvents.isEmpty()) {
                    if (var3) {
                        WatchEvent var7 = (WatchEvent) this.lastModifyEvents.get(var2);
                        if (var7 != null) {
                            assert var7.kind() == StandardWatchEventKinds.ENTRY_MODIFY;

                            ((CustomAbstractWatchKey.Event) var7).increment();
                            return;
                        }
                    } else {
                        this.lastModifyEvents.remove(var2);
                    }
                }

                if (var5 >= 512) {
                    var1 = StandardWatchEventKinds.OVERFLOW;
                    var3 = false;
                    var2 = null;
                }
            }

            CustomAbstractWatchKey.Event var10 = new CustomAbstractWatchKey.Event(var1, var2);
            if (var3) {
                this.lastModifyEvents.put(var2, var10);
            } else if (var1 == StandardWatchEventKinds.OVERFLOW) {
                this.events.clear();
                this.lastModifyEvents.clear();
            }

            this.events.add(var10);
            this.signal();
        }
    }

    public final List<WatchEvent<?>> pollEvents() {
        synchronized (this) {
            List var2 = this.events;
            this.events = new ArrayList();
            this.lastModifyEvents.clear();
            return var2;
        }
    }

    public final boolean reset() {
        synchronized (this) {
            if (this.state == CustomAbstractWatchKey.State.SIGNALLED && this.isValid()) {
                if (this.events.isEmpty()) {
                    this.state = CustomAbstractWatchKey.State.READY;
                } else {
                    this.watcher.enqueueKey(this);
                }
            }

            return this.isValid();
        }
    }

    static {
        OVERFLOW_EVENT = new CustomAbstractWatchKey.Event(StandardWatchEventKinds.OVERFLOW, (Object) null);
    }

    private static class Event<T> implements WatchEvent<T> {
        private final Kind<T> kind;
        private final T context;
        private int count;

        Event(Kind<T> var1, T var2) {
            this.kind = var1;
            this.context = var2;
            this.count = 1;
        }

        public Kind<T> kind() {
            return this.kind;
        }

        public T context() {
            return this.context;
        }

        public int count() {
            return this.count;
        }

        void increment() {
            ++this.count;
        }
    }

    private static enum State {
        READY,
        SIGNALLED;

        private State() {
        }
    }

}
