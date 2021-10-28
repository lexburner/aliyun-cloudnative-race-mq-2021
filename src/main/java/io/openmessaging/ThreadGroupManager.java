package io.openmessaging;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author jingfeng.xjf
 * @date 2021/9/15
 *
 * 聚合多个线程数据写入的管理器
 *
 * 核心思想：将 40 个线程分成 4 组，每组 10 个线程，聚合写入之后，等待最后一个线程一同 force，同时返回
 */
public class ThreadGroupManager {

    private final Lock lock;
    private final Condition condition;

    private final NativeMemoryByteBuffer writeBuffer;
    public FileChannel fileChannel;
    private volatile int count = 0;
    private volatile int total = 0;

    public ThreadGroupManager(int groupNo) {
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
        this.writeBuffer = new NativeMemoryByteBuffer(Constants.MAX_ONE_DATA_SIZE * Constants.NUMS_PERHAPS_IN_GROUP);
        try {
            String dataFilePath = Constants.ESSD_BASE_PATH + "/" + groupNo;
            File dataFile = new File(dataFilePath);
            if (!dataFile.exists()) {
                dataFile.createNewFile();
            }
            this.fileChannel = FileChannel.open(dataFile.toPath(), StandardOpenOption.READ,
                StandardOpenOption.WRITE);

            // recover
            if (this.fileChannel.size() > 0) {
                ByteBuffer idxWriteBuffer = ByteBuffer.allocate(Constants.IDX_GROUP_BLOCK_SIZE);
                long offset = 0;
                while (offset < this.fileChannel.size()) {
                    idxWriteBuffer.clear();
                    fileChannel.read(idxWriteBuffer, offset);
                    idxWriteBuffer.flip();
                    int topicId = idxWriteBuffer.get();
                    if (topicId < 0 || topicId > 100) {
                        break;
                    }
                    // 注意如果使用 unsafe，这里需要 reverse
                    int queueId = Short.reverseBytes(idxWriteBuffer.getShort());
                    if (queueId < 0 || queueId > 5000) {
                        break;
                    }
                    int len = Short.reverseBytes(idxWriteBuffer.getShort());
                    if (len <= 0 || len > Constants.MAX_ONE_DATA_SIZE) {
                        break;
                    }
                    Context.groupTopicManagers[topicId].addIndex(queueId,
                        new OffsetAndLen(offset + Constants.IDX_GROUP_BLOCK_SIZE, len),
                        this.fileChannel);
                    offset += Constants.IDX_GROUP_BLOCK_SIZE + len;
                }
            } else {
                if (groupNo < 4) {
                    Util.preAllocateFile(this.fileChannel, Constants.THREAD_GROUP_PRE_ALLOCATE_FILE_SIZE);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setTotal(int total) {
        this.total = total;
    }

    public long append(int topicNo, int queueId, ByteBuffer data) {
        AepManager aepManager = Context.threadAepManager.get();
        FixedLengthDramManager fixedLengthDramManager = Context.threadFixedLengthDramManager.get();

        int len = data.remaining();

        long aepPosition = -1;
        long writeBufferPosition = -1;

        // 优先使用 DRAM 缓存
        int coldCachePosition = fixedLengthDramManager.write(data);
        if (coldCachePosition == -1) {
            // 使用 AEP 缓存
            aepPosition = aepManager.write(data);
            writeBufferPosition = aepManager.getWriteBufferPosition() - len;
        }

        long ssdPosition = -1;

        lock.lock();
        try {
            count++;
            ssdPosition = fileChannel.position() + writeBuffer.position() + Constants.IDX_GROUP_BLOCK_SIZE;

            writeBuffer.put((byte)topicNo);
            writeBuffer.putShort((short)queueId);
            writeBuffer.putShort((short)len);
            writeBuffer.put(data);

            if (count != total) {
                condition.await(10, TimeUnit.SECONDS);
            } else {
                // 最后一个线程触发 force 刷盘
                force();
                count = 0;
                // 通知其他线程放行
                condition.signalAll();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        // 需要返回 queue 的长度
        return Context.groupTopicManagers[topicNo].append(queueId, ssdPosition, aepPosition, writeBufferPosition,
            coldCachePosition, len, fileChannel);
    }

    private void force() {
        // 4kb 对齐
        if (total > 5) {
            int position = writeBuffer.position();
            int mod = position % Constants._4kb;
            if (mod != 0) {
                writeBuffer.position(position + Constants._4kb - mod);
            }
        }

        writeBuffer.flip();
        try {
            fileChannel.write(writeBuffer.slice());
            fileChannel.force(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        writeBuffer.clear();
    }

}
