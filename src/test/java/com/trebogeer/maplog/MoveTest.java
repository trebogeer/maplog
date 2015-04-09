package com.trebogeer.maplog;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

/**
 * @author dimav
 *         Date: 4/8/15
 *         Time: 10:45 AM
 */
public class MoveTest {

    public static void main(String... args) {
        try {
            FileChannel fch = FileChannel.open(Paths.get("/tmp/2223"), StandardOpenOption.WRITE);
            FileLock fl = fch.tryLock();
            Files.move(Paths.get("/tmp/2223"), Paths.get("/tmp/2224"), StandardCopyOption.ATOMIC_MOVE);
            System.out.println(fl.isValid());
            Thread.sleep(3000000);
            fl.release();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
