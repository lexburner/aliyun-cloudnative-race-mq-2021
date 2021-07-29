package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;


public abstract class MessageQueue {
    /**
     * 写入一条信息；
     * 返回的long值为offset，用于从这个topic+queueId中读取这条数据
     * offset要求topic+queueId维度内严格递增，即第一条消息offset必须是0，第二条必须是1，第三条必须是2，第一万条必须是9999。
     * @param topic topic的值，总共有100个topic
     * @param queueId topic下队列的id，每个topic下不超过10000个
     * @param data 信息的内容，评测时会随机产生
     */
    public abstract long append(String topic, int queueId, ByteBuffer data);

    /**
     * 读取某个范围内的信息；
     * 返回值中的key为消息在Map中的偏移，从0开始，value为对应的写入data。读到结尾处没有新数据了，要求返回null。
     * @param topic topic的值
     * @param queueId topic下队列的id
     * @param offset 写入消息时返回的offset
     * @param fetchNum 读取消息个数，不超过100
     */
    public abstract Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum);
}
