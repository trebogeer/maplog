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

import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Base implementation class for watch keys.
 */
public abstract class CustomAbstractWatchService implements WatchService {

    private final LinkedBlockingDeque<WatchKey> pendingKeys = new LinkedBlockingDeque<>();
    private final WatchKey CLOSE_KEY = new CustomAbstractWatchKey((Path) null, (CustomAbstractWatchService) null) {
        public boolean isValid() {
            return true;
        }

        public void cancel() {
        }
    };
    private volatile boolean closed;
    private final Object closeLock = new Object();

    protected CustomAbstractWatchService() {
    }

    abstract WatchKey register(Path var1, WatchEvent.Kind<?>[] var2, WatchEvent.Modifier... var3) throws IOException;

    final void enqueueKey(WatchKey var1) {
        this.pendingKeys.offer(var1);
    }

    private void checkOpen() {
        if (this.closed) {
            throw new ClosedWatchServiceException();
        }
    }

    private void checkKey(WatchKey var1) {
        if (var1 == this.CLOSE_KEY) {
            this.enqueueKey(var1);
        }

        this.checkOpen();
    }

    public final WatchKey poll() {
        this.checkOpen();
        WatchKey var1 = this.pendingKeys.poll();
        this.checkKey(var1);
        return var1;
    }

    public final WatchKey poll(long var1, TimeUnit var3) throws InterruptedException {
        this.checkOpen();
        WatchKey var4 = this.pendingKeys.poll(var1, var3);
        this.checkKey(var4);
        return var4;
    }

    public final WatchKey take() throws InterruptedException {
        this.checkOpen();
        WatchKey var1 = this.pendingKeys.take();
        this.checkKey(var1);
        return var1;
    }

    final boolean isOpen() {
        return !this.closed;
    }

    final Object closeLock() {
        return this.closeLock;
    }

    abstract void implClose() throws IOException;

    public final void close() throws IOException {
        Object var1 = this.closeLock;
        synchronized (this.closeLock) {
            if (!this.closed) {
                this.closed = true;
                this.implClose();
                this.pendingKeys.clear();
                this.pendingKeys.offer(this.CLOSE_KEY);
            }
        }
    }
}
