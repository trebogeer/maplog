package com.trebogeer.maplog;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * @author dimav
 *         Date: 3/25/15
 *         Time: 1:49 PM
 */
public class LockTest {

    public static void main(String... agrs) {
        try (FileChannel fileChannel = FileChannel.open(Paths.get("/tmp/lock_test_1"), CREATE, READ, WRITE);
             /*FileLock fl = fileChannel.tryLock()*/) {
            System.out.println(fileChannel == null ? "Failed" : "Locked");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
