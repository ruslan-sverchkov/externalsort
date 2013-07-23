package com.example.externalsort;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;

/**
 * This is an integration test for the whole application.
 *
 * @author Ruslan Sverchkov
 */
public class ExternalSortTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testExternalSortDifferentFiles() throws Throwable {
        System.out.println("File size test");
        for (int intsNumber : new int[]{
                1,
                10,
                100,
                1000,
                10000,
                100000,
                1000000,
                10000000
        }) {
            testExternalSort(intsNumber, 16);
        }
    }

    @Test
    public void testExternalSortDifferentThreadsNumber() throws Throwable {
        System.out.println("Multithreading test");
        for (int threadsNumber : new int[]{1, 2, 4, 8, 16, 32, 64}) {
            testExternalSort(10000000, threadsNumber);
        }
    }

    protected void testExternalSort(int intsNumber, int threadsNumber) throws Throwable {
        File testData = folder.newFile();
        try (DataOutputStream s = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(testData)))) {
            for (int i = intsNumber; i > 0; i--) {
                s.writeInt(i);
            }
        }
        File expected = folder.newFile();
        try (DataOutputStream s = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(expected)))) {
            for (int i = 1; i <= intsNumber; i++) {
                s.writeInt(i);
            }
        }
        long time = System.currentTimeMillis();
        ExternalSort.main(testData.getAbsolutePath(), String.valueOf(threadsNumber));
        long taken = System.currentTimeMillis() - time;
        System.out.format("integers: %,12d, threads: %,2d, milliseconds: %,6d%n", intsNumber, threadsNumber, taken);
        Assert.assertEquals(getMD5(testData), getMD5(expected));
    }

    protected String getMD5(File file) throws IOException {
        try(InputStream s = new BufferedInputStream(new FileInputStream(file))) {
            return DigestUtils.md5Hex(s);
        }
    }

}