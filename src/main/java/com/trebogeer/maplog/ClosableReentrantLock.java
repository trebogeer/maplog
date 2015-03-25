package com.trebogeer.maplog;

import java.util.concurrent.locks.ReentrantLock;

/**
 * @author dimav
 *         Date: 3/25/15
 *         Time: 10:41 AM
 */
public class ClosableReentrantLock extends ReentrantLock implements AutoCloseable {

    @Override
    public final void close() throws Exception {
         unlock();
    }

    public final ClosableReentrantLock l0ck() {
        super.lock();
        return this;
    }
}
