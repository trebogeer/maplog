package com.trebogeer.maplog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * @author dimav
 *         Date: 3/18/15
 *         Time: 12:03 PM
 */
public final class TestUtils {

    static final Logger utlogger = LoggerFactory.getLogger("UNIT.TEST");

    static int total_workers = 2;
    static int work_size_per_worker = 10000;

    public static final int BUFFER = 0x2000;

    static final String key_template = "nisp_ghyu_5012%d?hei=624&wid=624&op_sharpen=1";

    private TestUtils() {
    }

    public static void pipe(final InputStream source, final OutputStream target) throws IOException {
        byte[] buf = new byte[BUFFER];
        while (true) {
            int r = source.read(buf);
            if (r == -1) {
                break;
            }
            target.write(buf, 0, r);
            target.flush();
        }
    }

    public static void deleteDir(String p) throws IOException {

        Path directory = Paths.get(p);
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }

        });

    }

}
