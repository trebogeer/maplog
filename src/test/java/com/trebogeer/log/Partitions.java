package com.trebogeer.log;

import javax.swing.filechooser.FileSystemView;
import java.io.File;

/**
 * @author dimav
 *         Date: 3/18/15
 *         Time: 2:23 PM
 */
public class Partitions {

    public static void main(String... args){
        FileSystemView fsv = FileSystemView.getFileSystemView();
        File[] files = File.listRoots();

        File[] roots = fsv.getRoots();
        for (int i = 0; i < roots.length; i++) {
            System.out.println("Root: " + roots[i]);
        }

        // TODO cat /proc/mounts

//        for (File fi : files) {
//            if (fsv.getSystemTypeDescription(fi).contains("Local Disk")
//                    || fsv.getSystemTypeDescription(fi).contains(
//                    "Removable Disk")) {
//                System.out.println(fsv.getSystemDisplayName(fi));
//            }
//        }
    }
}
