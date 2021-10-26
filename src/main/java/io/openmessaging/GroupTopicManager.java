package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author jingfeng.xjf
 * @date 2021/9/15
 */
public class GroupTopicManager {

    static ThreadLocal<ArrayMap> mapCache = ThreadLocal.withInitial(
            () -> new ArrayMap(Constants.MAX_FETCH_NUM));
    static ThreadLocal<List<ByteBuffer>> retBuffers = ThreadLocal.withInitial(() -> {
        List<ByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < Constants.MAX_FETCH_NUM; i++) {
            buffers.add(ByteBuffer.allocateDirect(Constants.MAX_ONE_DATA_SIZE));
        }
        return buffers;
    });
    /**
     * 1 热读队列
     * 0 未知
     * -1 冷读队列
     */
//    public int[] queueCategory;

    public int topicNo;
    private List<OffsetAndLen>[] queue2index2Offset;
    private FileChannel dataFileChannel;
    private Semaphore[] semaphores;

    public GroupTopicManager(int topicNo) {
        this.topicNo = topicNo;
//        this.queueCategory = new int[Constants.QUEUE_NUM];
        queue2index2Offset = new ArrayList[Constants.QUEUE_NUM];
        for (int i = 0; i < Constants.QUEUE_NUM; i++) {
            queue2index2Offset[i] = new ArrayList<>();
        }
        semaphores = new Semaphore[Constants.MAX_FETCH_NUM];
        for (int i = 0; i < Constants.MAX_FETCH_NUM; i++) {
            semaphores[i] = new Semaphore(0);
        }
    }

    public long append(int queueId, long ssdPosition, long aepPosition, long writeBufferPosition, int coldCachePosition,
                       int len, FileChannel dataFileChannel) {
        try {
            // 保存索引
            List<OffsetAndLen> index2Offset = queue2index2Offset[queueId];
            long curOffset = index2Offset.size();
            OffsetAndLen offsetAndLen = Context.threadAepManager.get().offerOffsetAndLen();
            offsetAndLen.setSsdOffset(ssdPosition);
            offsetAndLen.setAepOffset(aepPosition);
            offsetAndLen.setColdReadOffset(coldCachePosition);
            offsetAndLen.setAepWriteBufferOffset(writeBufferPosition);
            offsetAndLen.setLength(len);
            index2Offset.add(offsetAndLen);
            if (this.dataFileChannel == null) {
                this.dataFileChannel = dataFileChannel;
            }
            return curOffset;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Integer, ByteBuffer> getRange(int queueId, int offset, int fetchNum) {
//        if (queueCategory[queueId] == 0) {
//            queueCategory[queueId] = offset == 0 ? -1 : 1;
//        }
        try {
            ArrayMap ret = mapCache.get();
            ret.clear();
            AepManager aepManager = Context.threadAepManager.get();

            List<OffsetAndLen> offsetAndLens = queue2index2Offset[queueId];
            if (offsetAndLens == null) {
                return ret;
            }

            int size = offsetAndLens.size();

            for (int i = 0; i < fetchNum && i + offset < size; i++) {
                ByteBuffer retBuffer = retBuffers.get().get(i);
                retBuffer.clear();
                OffsetAndLen offsetAndLen = offsetAndLens.get(i + offset);
                retBuffer.limit(offsetAndLen.getLength());
                aepManager.read(offsetAndLen, i, retBuffer, ret, this.dataFileChannel, semaphores[i]);
            }
            for (int i = 0; i < fetchNum && i + offset < size; i++) {
                semaphores[i].tryAcquire(1, 3, TimeUnit.SECONDS);
            }
            return ret;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void addIndex(int queueId, OffsetAndLen offsetAndLen, FileChannel dataFileChannel) {
        if (this.dataFileChannel == null) {
            this.dataFileChannel = dataFileChannel;
        }
        List<OffsetAndLen> offsetAndLens = queue2index2Offset[queueId];
        offsetAndLens.add(offsetAndLen);
    }
}
