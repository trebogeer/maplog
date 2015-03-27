package com.trebogeer.maplog;

import com.trebogeer.maplog.hash.MurMur3;

import java.util.HashSet;
import java.util.Set;

import static com.trebogeer.maplog.TestUtils.utlogger;

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
            if (!ints.add(i)) {
                utlogger.info("Collision " + i);
            }
        }
        else {
            for (int i = 0; i < n; i++)
                permutation(prefix + str.charAt(i), str.substring(0, i) + str.substring(i + 1, n), ints);
        }
    }

    public static void main(String... args) {
        String ab = "abcdefghijklmnopqrstuvwxyz123456789";
        permutation(ab, new HashSet<>());
    }

}
