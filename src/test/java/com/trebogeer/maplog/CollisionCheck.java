package com.trebogeer.maplog;

import java.util.HashSet;
import java.util.Set;

/**
 * @author dimav
 *         Date: 3/16/15
 *         Time: 4:31 PM
 */
public class CollisionCheck {

    public static void permutation(String str, Set<Long> ints) {
        permutation("", str, ints);
    }

    private static void permutation(String prefix, String str, Set<Long> ints) {
        int n = str.length();
        if (n == 0) {
            long i = MurMur3.MurmurHash3_x64_64(prefix.getBytes(), 127);
            if (!ints.add(i)){
                System.out.println("Collision");
            }
        }
        else {
            for (int i = 0; i < n; i++)
                permutation(prefix + str.charAt(i), str.substring(0, i) + str.substring(i + 1, n), ints);
        }
    }

    public static void main(String... args) {
        String ab = "abcdefghijklmnopqrstuvwxyz12345";
        permutation(ab, new HashSet<>());
    }

}
