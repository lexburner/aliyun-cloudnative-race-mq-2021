package io.openmessaging;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import sun.misc.JavaNioAccess;
import sun.misc.SharedSecrets;
import sun.misc.Unsafe;
import sun.misc.VM;

/**
 * @author jingfeng.xjf
 * @date 2021/9/17
 */
public class Util {

    public static int parseInt(String input) {
        switch (input.length()) {
            case 7: {
                return (input.charAt(5) - '0') * 10 + (input.charAt(6) - '0');
            }
            case 6: {
                return input.charAt(5) - '0';
            }

        }
        throw new RuntimeException("unknown topic " + input);
    }

    public static final Unsafe unsafe = getUnsafe();

    private static sun.misc.Unsafe getUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe)field.get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static BufferPoolMXBean getDirectBufferPoolMBean() {
        return ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)
            .stream()
            .filter(e -> e.getName().equals("direct"))
            .findFirst().get();
    }

    public static void preAllocateFile(FileChannel fileChannel, long threadGroupPerhapsFileSize) {
        int bufferSize = 4 * 1024;
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bufferSize);
        for (int i = 0; i < bufferSize; i++) {
            byteBuffer.put((byte)-1);
        }
        byteBuffer.flip();
        long loopTimes = threadGroupPerhapsFileSize / bufferSize;
        for (long i = 0; i < loopTimes; i++) {
            try {
                fileChannel.write(byteBuffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byteBuffer.flip();
        }
        try {
            fileChannel.force(true);
            fileChannel.position(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public JavaNioAccess.BufferPool getNioBufferPool() {
        return SharedSecrets.getJavaNioAccess().getDirectBufferPool();
    }

    /**
     * -XX:MaxDirectMemorySize=60M
     */
    public void testGetMaxDirectMemory() {
        System.out.println(Runtime.getRuntime().maxMemory() / 1024.0 / 1024.0);
        System.out.println(VM.maxDirectMemory() / 1024.0 / 1024.0);
        System.out.println(getDirectBufferPoolMBean().getTotalCapacity() / 1024.0 / 1024.0);
        System.out.println(getNioBufferPool().getTotalCapacity() / 1024.0 / 1024.0);
    }

    public static void analysisMemory() {
        System.out.printf("totalMemory: %sM freeMemory: %sM usedMemory: %sM directMemoryUsed: %sM\n",
            byte2M(Runtime.getRuntime().totalMemory()), byte2M(Runtime.getRuntime().freeMemory()),
            byte2M(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()),
            byte2M(Util.getDirectBufferPoolMBean().getMemoryUsed()));
    }

    public static void analysisMemoryOnce() {
        System.out.printf("==specialPrint totalMemory: %sM freeMemory: %sM usedMemory: %sM directMemoryUsed: %sM\n",
            byte2M(Runtime.getRuntime().totalMemory()), byte2M(Runtime.getRuntime().freeMemory()),
            byte2M(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()),
            byte2M(Util.getDirectBufferPoolMBean().getMemoryUsed()));
    }

    public static void printParams() {
        System.out.println("Constants.GROUPS=" + Constants.GROUPS);
        System.out.println(
            "Constants.THREAD_COLD_READ_BUFFER_SIZE=" + Util.byte2M(Constants.THREAD_COLD_READ_BUFFER_SIZE) + "M");
        System.out.println("Constants.THREAD_AEP_SIZE=" + Util.byte2M(Constants.THREAD_AEP_HOT_CACHE_WINDOW_SIZE) + "M");
        System.out.println(
            "Constants.THREAD_HOT_WRITE_BUFFER_SIZE=" + Util.byte2M(Constants.THREAD_AEP_WRITE_BUFFER_SIZE) + "M");
        System.out.println(
            "Constants.THREAD_COLD_READ_THRESHOLD_SIZE=" + Util.byte2KB(Constants.THREAD_COLD_READ_THRESHOLD_SIZE) + "KB");
    }

    public static long byte2M(long x) {
        return x / 1024 / 1024;
    }

    public static long byte2KB(long x) {
        return x / 1024;
    }

}
