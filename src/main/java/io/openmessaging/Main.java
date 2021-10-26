package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import sun.misc.VM;
import sun.nio.ch.DirectBuffer;

/**
 * @author jingfeng.xjf
 * @date 2021/9/15
 */
public class Main {

    public static void main(String[] args) throws Exception {
        testMmap();
    }

    public static void testMmap() throws Exception {
        File file = new File("/Users/xujingfeng/test.txt");
        file.createNewFile();
        FileChannel ch = new RandomAccessFile(file, "rw").getChannel();
        MappedByteBuffer map = ch.map(MapMode.READ_WRITE, 0, 1L * 1024);
        System.out.println(map instanceof DirectBuffer);
    }

    public static void testCLQ() {
        ConcurrentLinkedDeque<Integer> clq = new ConcurrentLinkedDeque<>();
        clq.offer(1);
        clq.offer(2);
        clq.offer(3);
        clq.offer(4);
        System.out.println(clq.poll());
        System.out.println(clq.poll());
        System.out.println(clq.poll());
        System.out.println(clq.poll());
        System.out.println(clq.poll());

    }

    public static void testFill4kb(int position) {
        int ret = position;
        int mod = position % Constants._4kb;
        if (mod != 0) {
            ret = position + Constants._4kb - mod;
        }
        System.out.println(ret);
    }

    private static void test9() {
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(2);
        byteBuffer.put((byte)-1);
        byteBuffer.put((byte)-1);
        byteBuffer.flip();
        System.out.println(byteBuffer.getShort());
        byteBuffer.flip();
        System.out.println(Short.reverseBytes(byteBuffer.getShort()));
    }

    private static void testSemaphore() throws InterruptedException {
        Semaphore semaphore = new Semaphore(0);
        new Thread(() -> {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            semaphore.release(1);
        }).start();
        new Thread(() -> {
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            semaphore.release(1);
        }).start();
        semaphore.acquire();
        System.out.println("====1");
        semaphore.acquire();
        System.out.println("====2");
    }

    private static void fillSsdFile(long threadGroupPerhapsFileSize) throws Exception {
        File file = new File("/Users/xujingfeng/test2.data");
        FileChannel fileChannel = new RandomAccessFile(file, "rw").getChannel();
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4 * 1024);
        for (int i = 0; i < 1024; i++) {
            byteBuffer.putInt(0);
        }
        byteBuffer.flip();
        long loopTimes = threadGroupPerhapsFileSize / (4 * 1024);
        for (long i = 0; i < loopTimes; i++) {
            fileChannel.write(byteBuffer);
            byteBuffer.flip();
        }
    }

    public static void modifyMemory() throws Exception {
        Object bits = Util.unsafe.allocateInstance(Class.forName("java.nio.Bits"));
        Field maxMemory = bits.getClass().getDeclaredField("maxMemory");
        maxMemory.setAccessible(true);
        maxMemory.set(bits, 8L * 1024 * 1024 * 1024);
    }

    public static void allocateMemory2() {
        long address = Util.unsafe.allocateMemory(2);
        System.out.println(Util.unsafe.getByte(address));
        System.out.println(Util.unsafe.getByte(address + 1));
        System.out.println(Util.unsafe.getByte(address + 444));
    }

    public static void allocateMemory() throws NoSuchFieldException, IllegalAccessException {
        Field directMemoryField = VM.class.getDeclaredField("directMemory");
        directMemoryField.setAccessible(true);
        directMemoryField.set(new VM(), 80 * 1024 * 1024);
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(50 * 1024 * 1024);
    }

    public static void testUnsafe3() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Object o = Util.unsafe.allocateInstance(Class.forName("moe.cnkirito.TestProtect"));
        Object o1 = Class.forName("moe.cnkirito.TestProtect").newInstance();
    }

    // 测试 unsafe allocate 是堆外还是堆内内存

    public static void testUnsafe2() {
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(4 * 4);
        long directBufferAddress = ((DirectBuffer)directBuffer).address();
        Util.unsafe.putShort(directBufferAddress, (short)1);
        System.out.println(Util.unsafe.getShort(directBufferAddress));
        directBuffer.position(0);
        directBuffer.limit(16);
        System.out.println(Short.reverseBytes(directBuffer.getShort()));
    }

    static void putIntB(long a, int x) {
        Util.unsafe.putByte(a, int3(x));
        Util.unsafe.putByte(a + 1, int2(x));
        Util.unsafe.putByte(a + 2, int1(x));
        Util.unsafe.putByte(a + 3, int0(x));
    }

    static void putIntL(long a, int x) {
        Util.unsafe.putByte(a, int0(x));
        Util.unsafe.putByte(a + 1, int1(x));
        Util.unsafe.putByte(a + 2, int2(x));
        Util.unsafe.putByte(a + 3, int3(x));
    }

    private static byte int3(int x) {return (byte)(x >> 24);}

    private static byte int2(int x) {return (byte)(x >> 16);}

    private static byte int1(int x) {return (byte)(x >> 8);}

    private static byte int0(int x) {return (byte)(x);}

    public static void testUnsafe1() {
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(4 * 4);
        ByteBuffer heapBuffer = ByteBuffer.allocate(4 * 4);
        heapBuffer.putInt(1);
        heapBuffer.putInt(2);
        heapBuffer.putInt(3);
        heapBuffer.putInt(4);
        long directBufferAddress = ((DirectBuffer)directBuffer).address();
        Util.unsafe.copyMemory(heapBuffer.array(), 16, null, directBufferAddress, 16);
        directBuffer.position(0);
        directBuffer.limit(16);
        System.out.println(directBuffer.getInt());
        System.out.println(directBuffer.getInt());
        System.out.println(directBuffer.getInt());
        System.out.println(directBuffer.getInt());
    }

    public static void testThreadLocalRandom() {
        int total = 100;
        int trueTime = 0;
        for (int i = 0; i < total; i++) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                trueTime++;
            }
        }
        System.out.println(trueTime);
    }

    public void testSignalAll() {
        Main main = new Main();
        for (int i = 0; i < 40; i++) {
            final int n = i;
            new Thread(() -> {
                try {
                    if (n == 39) {
                        Thread.sleep(3000);
                    }
                    main.initThread();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();
    int count = 0;

    public void initThread() throws InterruptedException {
        lock.lock();
        try {
            count++;
            if (count != 40) {
                boolean await = condition.await(10000, TimeUnit.SECONDS);
                if (await) {
                    System.out.printf("%s is notified\n", Thread.currentThread().getName());
                }
            } else {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public static void testXms() throws InterruptedException {
        System.out.println(
            Runtime.getRuntime().totalMemory() / 1024 / 1024 + " " + Runtime.getRuntime().freeMemory() / 1024 / 1024);
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            byteBuffers.add(ByteBuffer.allocate(10 * 1024 * 1024));
            Thread.sleep(1000);
            System.out.println(Runtime.getRuntime().totalMemory() / 1024 / 1024 + " "
                + Runtime.getRuntime().freeMemory() / 1024 / 1024);
        }
        Thread.sleep(Integer.MAX_VALUE);
    }

    public static void test8() {
        ByteBuffer buffer = ByteBuffer.allocate(12);
        System.out.println(buffer.capacity());
        buffer.putInt(1);
        buffer.putInt(1);
        buffer.putInt(1);
        System.out.println(buffer.capacity());
        buffer.flip();
        System.out.println(buffer.capacity());

    }

    public static void test7() throws InterruptedException {
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {}

            System.exit(0);
        }).start();

        new CountDownLatch(1).await();
    }

    public static void test6() {
        ByteBuffer data = ByteBuffer.allocate(4);

        ByteBuffer buffer = ByteBuffer.allocate(12);
        buffer.putInt(1);
        buffer.putInt(2);
        buffer.putInt(3);

        System.out.println(buffer.position());

        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(4);
        duplicate.limit(duplicate.position() + data.limit());

        data.put(duplicate);
        data.flip();

        System.out.println(data.getInt());
        System.out.println(buffer.position());
    }

    public static void test5() {
        ByteBuffer data = ByteBuffer.allocate(4);
        data.putInt(2);
        data.flip();
        System.out.println(data.remaining());

        ByteBuffer buffer = ByteBuffer.allocate(12);

        buffer.put(data);

        System.out.println(buffer.capacity());
        System.out.println(buffer.position());
        System.out.println(buffer.remaining());
        System.out.println(buffer.hasRemaining());
    }

    public static void test4() throws IOException {
        File file = new File("/pmem/testFile");
        file.createNewFile();
        FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
            StandardOpenOption.WRITE);
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(1);
        buffer.flip();
        int write = fileChannel.write(buffer, 4);
        System.out.println(write);

    }

    public static void test3() {
        System.out.println(Byte.MAX_VALUE);
        System.out.println(Short.MAX_VALUE);
        System.out.println(Integer.MAX_VALUE);
        byte a = 99;
        int b = a;
        System.out.println(b);
    }

    public static void test2() {
        int[] nums = new int[10];
        for (int i = 10; i < 50; i++) {
            nums[i % 10]++;
        }
        for (int num : nums) {
            System.out.println(num);
        }
    }

    public void test1() throws InterruptedException {
        Lock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        new Thread(() -> {
            lock.lock();
            try {
                boolean await = condition.await(1, TimeUnit.SECONDS);
                System.out.println(await);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }).start();

        Thread.sleep(500);
        lock.lock();
        try {
            condition.signalAll();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

}
