package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;

/**
 * @author jingfeng.xjf
 * @date 2021/9/18
 */
public class AepBenchmark {

    public void testAepWriteIops()
        throws Exception {
        int threadNum = 40;
        String workDir = "/pmem";
        long totalSize = 125L * 1024 * 1024 * 1024;
        int bufferSize = 100 * 1024 * 1024;

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            final int threadNo = i;
            new Thread(() -> {
                try {
                    File file = new File(workDir + "/daofeng_" + threadNo);
                    RandomAccessFile rw = new RandomAccessFile(file, "rw");
                    FileChannel fileChannel = rw.getChannel();
                    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
                    for (int j = 0; j < bufferSize; j++) {
                        byteBuffer.put((byte)1);
                    }
                    long loopTime = totalSize / bufferSize / threadNum;
                    long maxFileLength = 60L * 1024 * 1024 * 1024 / 40 - 1024 * 1024;
                    long position = 0;
                    for (int t = 0; t < loopTime; t++) {
                        byteBuffer.flip();
                        fileChannel.write(byteBuffer, position % maxFileLength);
                        position += bufferSize;
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " write " + totalSize + " bufferSize " + bufferSize + " cost " + (
                System.currentTimeMillis()
                    - start) + " ms");
    }

    public void testRead2() throws Exception {
        int bufferSize = 200 * 1024 * 1024;
        File file = new File("/essd");
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        fileChannel.read(byteBuffer);
    }

    public void testRead3() throws Exception {
        File file = new File("/essd");
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocate(50 * 1024 * 1024);

        ByteBuffer directByteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        for (int i = 0; i < 12800; i++) {
            directByteBuffer.clear();
            fileChannel.read(directByteBuffer, i * 4 * 1024);
            directByteBuffer.flip();
            byteBuffer.put(directByteBuffer);
        }
    }

    public void testAepReadIops()
        throws Exception {
        int threadNum = 40;
        String workDir = "/pmem";
        long totalSize = 125L * 1024 * 1024 * 1024;
        int bufferSize = 13 * 1024;

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            final int threadNo = i;
            new Thread(() -> {
                try {
                    File file = new File(workDir + "/daofeng_" + threadNo);
                    RandomAccessFile rw = new RandomAccessFile(file, "rw");
                    FileChannel fileChannel = rw.getChannel();
                    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
                    long loopTime = totalSize / bufferSize / threadNum;
                    long maxFileLength = 60L * 1024 * 1024 * 1024 / 40 - 1024 * 1024 * 2;
                    long position = 0;
                    for (int t = 0; t < loopTime; t++) {
                        byteBuffer.clear();
                        fileChannel.read(byteBuffer, position % maxFileLength);
                        position += bufferSize;
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " read " + totalSize + " bufferSize " + bufferSize + " cost " + (
                System.currentTimeMillis()
                    - start) + " ms");
    }

    public void testWrite(String workDir, long totalSize, final int threadNum, final int bufferSize,
        final int testRound)
        throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        long start = System.currentTimeMillis();
        for (int i = 0; i < threadNum; i++) {
            final int threadNo = i;
            new Thread(() -> {
                try {
                    File file = new File(workDir + "/" + testRound + "_" + threadNo);
                    RandomAccessFile rw = new RandomAccessFile(file, "rw");
                    FileChannel fileChannel = rw.getChannel();
                    ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
                    for (int j = 0; j < bufferSize; j++) {
                        byteBuffer.put((byte)1);
                    }
                    long loopTime = totalSize / bufferSize / threadNum;
                    for (int t = 0; t < loopTime; t++) {
                        byteBuffer.flip();
                        fileChannel.write(byteBuffer);
                    }
                    countDownLatch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        countDownLatch.await();
        System.out.println(
            "threadNum " + threadNum + " write " + totalSize + " bufferSize " + bufferSize + " cost " + (
                System.currentTimeMillis()
                    - start) + " ms");

    }

    public void testWriteEssd() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(40);

        FileChannel[] fileChannels = new FileChannel[40];
        for (int i = 0; i < 40; i++) {
            String dataFilePath = "/essd/" + i;
            File dataFile = new File(dataFilePath);
            dataFile.createNewFile();
            fileChannels[i] = FileChannel.open(dataFile.toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 40; i++) {
            final int threadNo = i;
            new Thread(() -> {
                ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 1024);
                for (int j = 0; j < 4 * 1024; j++) {
                    buffer.put((byte)1);
                }
                buffer.flip();

                for (int t = 0; t < 384 * 1024; t++) {
                    try {
                        fileChannels[threadNo].write(buffer);
                        buffer.flip();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
        System.out.println("ssd write 40 * 384 * 4 * 1024 * 1024 cost " + (System.currentTimeMillis() - start) + " ms");
    }

    public void testWriteAep() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(40);

        FileChannel[] fileChannels = new FileChannel[40];
        for (int i = 0; i < 40; i++) {
            String dataFilePath = "/pmem/" + i;
            File dataFile = new File(dataFilePath);
            dataFile.createNewFile();
            fileChannels[i] = FileChannel.open(dataFile.toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < 40; i++) {
            final int threadNo = i;
            new Thread(() -> {
                ByteBuffer buffer = ByteBuffer.allocateDirect(4 * 1024);
                for (int j = 0; j < 4 * 1024; j++) {
                    buffer.put((byte)1);
                }
                buffer.flip();

                for (int t = 0; t < 384 * 1024; t++) {
                    try {
                        fileChannels[threadNo].write(buffer);
                        buffer.flip();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
        System.out.println("aep write 40 * 384 * 4 * 1024 * 1024 cost " + (System.currentTimeMillis() - start) + " ms");
    }

    public void testWriteDram(ByteBuffer[] byteBuffers) throws InterruptedException {
        CountDownLatch countDownLatch = new CountDownLatch(40);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 40; i++) {
            final int threadNo = i;
            new Thread(() -> {
                ByteBuffer buffer = byteBuffers[threadNo];
                for (int t = 0; t < 24; t++) {
                    buffer.clear();
                    for (int j = 0; j < 64 * 1024 * 1024; j++) {
                        buffer.put((byte)1);
                    }
                }
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
        System.out.println("dram write 40 * 24 * 64 * 1024 * 1024 cost " + (System.currentTimeMillis() - start) + " ms");
    }

    public void test() {
        AepBenchmark aepBenchmark = new AepBenchmark();
        ByteBuffer[] byteBuffers = new ByteBuffer[40];
        for (int i = 0; i < 40; i++) {
            byteBuffers[i] = ByteBuffer.allocate(64 * 1024 * 1024);
        }
        try {
            long start = System.currentTimeMillis();
            CountDownLatch countDownLatch = new CountDownLatch(3);
            new Thread(() -> {
                try {
                    aepBenchmark.testWriteAep();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            }).start();

            new Thread(() -> {
                try {
                    aepBenchmark.testWriteEssd();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            }).start();

            new Thread(() -> {
                try {
                    aepBenchmark.testWriteDram(byteBuffers);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                countDownLatch.countDown();
            }).start();

            countDownLatch.await();
            System.out.println("=== total cost: " + (System.currentTimeMillis() - start) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static void main(String[] args) throws Exception {

    }

}
