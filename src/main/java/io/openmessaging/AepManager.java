package io.openmessaging;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

/**
 * @author jingfeng.xjf
 * @date 2021/9/22
 *
 * AEP 管理器，
 * AepManager 核心思想：使用滑动窗口的思想进行缓存管理
 * writeBuffer 有两层作用：
 * 1. 作为 AEP 聚合刷盘的写入缓冲
 * 2. 作为热数据的 dram 缓存
 */
public class AepManager {

    private final FileChannel fileChannel;
    /**
     * AEP 写入缓冲 & DRAM 热读缓存 的大小
     */
    private final int writeBufferSize;
    /**
     * AEP 写入缓冲 & DRAM 热读缓存
     */
    private final NativeMemoryByteBuffer writeBuffer;
    /**
     * AEP 滑动窗口的大小
     */
    private final long windowSize;
    /**
     * 提前将 Runtime 需要用到的索引对象分配好
     */
    private final OffsetAndLen[] offsetAndLens;
    private int offsetAndLenIndex = 0;
    /**
     * getRange 阶段，ssd 可以并发读
     */
    private final ExecutorService ssdReadExecutorService;
    /**
     * AEP 的逻辑长度
     */
    public long globalPosition;
    /**
     * DRAM 热读缓存的逻辑长度
     */
    public long writeBufferPosition;


    public AepManager(String id, int writeBufferSize, long aepWindowSize) {
        try {
            this.offsetAndLens = new OffsetAndLen[Constants.THREAD_MSG_NUM];
            for (int i = 0; i < Constants.THREAD_MSG_NUM; i++) {
                this.offsetAndLens[i] = new OffsetAndLen();
            }
            this.windowSize = aepWindowSize;
            this.globalPosition = 0;
            this.writeBufferPosition = 0;
            this.writeBufferSize = writeBufferSize;
            this.writeBuffer = new NativeMemoryByteBuffer(writeBufferSize);
            String path = Constants.AEP_BASE_PATH + "/" + id;
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            this.fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            // 预分配
            Util.preAllocateFile(this.fileChannel, Constants.THREAD_AEP_HOT_CACHE_PRE_ALLOCATE_SIZE);

            ssdReadExecutorService = Executors.newFixedThreadPool(8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public long write(ByteBuffer data) {
        try {
            // 待写入数据的大小
            int len = data.remaining();
            // 文件的物理写指针
            long windowPosition = this.globalPosition % this.windowSize;
            // 内存的写指针
            long bufferWritePosition = this.writeBuffer.position();
            // 写缓存
            if (len <= this.writeBuffer.remaining()) {
                // 考虑缓存块在 aep 上折行的问题
                if (windowPosition + bufferWritePosition + len <= this.windowSize) {
                    // 大多数情况下，直接写入缓存，返回逻辑位移=文件逻辑写指针+内存写地址
                    this.writeBuffer.put(data);
                    this.writeBufferPosition += len;
                    return globalPosition + bufferWritePosition;
                } else {
                    // 少数情况下，需要考虑缓存块写入之后要换行的问题，需要先把已有的缓存落盘，换一行写。返回逻辑位移=文件逻辑写指针（因为内存块偏移肯定是0）
                    this.writeBufferPosition += this.writeBuffer.remaining() + len;

                    this.writeBuffer.flip();
                    this.fileChannel.write(this.writeBuffer.slice(), windowPosition);
                    this.globalPosition += this.windowSize - windowPosition;
                    this.writeBuffer.clear();
                    this.writeBuffer.put(data);
                    return globalPosition;
                }
            } else { // 缓存不够写入了，先把缓存落盘，再写入缓存
                this.writeBufferPosition += this.writeBuffer.remaining() + len;
                // 缓存块落盘
                this.writeBuffer.flip();
                int size = this.fileChannel.write(this.writeBuffer.slice(), windowPosition);
                this.globalPosition += size;
                this.writeBuffer.clear();

                this.writeBuffer.put(data);
                windowPosition = this.globalPosition % this.windowSize;
                if (windowPosition + len <= this.windowSize) {
                    // 多数情况，新的缓存块没有折行 返回逻辑位移=文件逻辑写指针（因为内存块刚落盘过，肯定是0）
                    return this.globalPosition;
                } else {
                    // 少数情况，新的缓存块刚写入第一个数据，就超过了该行的剩余大小，需要折行，修改逻辑位移即可
                    this.globalPosition += this.windowSize - windowPosition;
                    return this.globalPosition;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 不方便同时返回 aepPosition 和 writeBufferPosition，单独提供该方法作为一个折中
     */
    public long getWriteBufferPosition() {
        return writeBufferPosition;
    }

    public ByteBuffer readWriteBuffer(long position, int len) {
        int bufferPosition = (int) (position % this.writeBufferSize);
        ByteBuffer duplicate = this.writeBuffer.duplicate();
        duplicate.limit(bufferPosition + len);
        duplicate.position(bufferPosition);
        return duplicate.slice();
    }

    public OffsetAndLen offerOffsetAndLen() {
        return this.offsetAndLens[offsetAndLenIndex++];
    }

    public void read(OffsetAndLen offsetAndLen, int i, ByteBuffer reUseBuffer, ArrayMap ret,
                     FileChannel dataFileChannel, Semaphore semaphore) throws Exception {

        // aep write cache
        long bufferStartPosition = Math.max(0, this.writeBufferPosition - this.writeBufferSize);
        if (offsetAndLen.getAepWriteBufferOffset() >= bufferStartPosition) {
            ByteBuffer duplicate = this.readWriteBuffer(offsetAndLen.getAepWriteBufferOffset(),
                    offsetAndLen.getLength());
            ret.put(i, duplicate);
            semaphore.release(1);
//            Context.directDram.incrementAndGet();
            return;
        }

        // cold read cache
        if (offsetAndLen.getColdReadOffset() != -1) {
            FixedLengthDramManager fixedLengthDramManager = Context.threadFixedLengthDramManager.get();
            ByteBuffer coldCache = fixedLengthDramManager.read(offsetAndLen.getColdReadOffset(),
                    offsetAndLen.getLength());
            ret.put(i, coldCache);
            semaphore.release(1);
//            Context.heapDram.incrementAndGet();
            return;
        }

        // aep
        long aepStartPosition = Math.max(0, this.globalPosition - this.windowSize);
        if (offsetAndLen.getAepOffset() >= aepStartPosition) {
            long physicalPosition = offsetAndLen.getAepOffset() % this.windowSize;
            ssdReadExecutorService.execute(() -> {
                try {
                    this.fileChannel.read(reUseBuffer, physicalPosition);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                reUseBuffer.flip();
                ret.put(i, reUseBuffer);
                semaphore.release(1);
            });
//            Context.aep.incrementAndGet();
            return;
        }

        // 未命中，降级到 ssd 读
        ssdReadExecutorService.execute(() -> {
            try {
                dataFileChannel.read(reUseBuffer, offsetAndLen.getSsdOffset());
            } catch (IOException e) {
                e.printStackTrace();
            }
            reUseBuffer.flip();
            ret.put(i, reUseBuffer);
            semaphore.release(1);
        });
//        Context.ssd.incrementAndGet();
    }

}
