package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这是一个简单的基于内存的实现，以方便选手理解题意；
 * 实际提交时，请维持包名和类名不变，把方法实现修改为自己的内容；
 */
public class DefaultMessageQueueImpl extends MessageQueue {
    ConcurrentHashMap<String, Map<Integer, Long>> appendOffset = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Map<Integer, Map<Long, ByteBuffer>>> appendData = new ConcurrentHashMap<>();

    // getOrPutDefault 若指定key不存在，则插入defaultValue并返回
    private <K, V> V getOrPutDefault(Map<K, V> map, K key, V defaultValue){
        V retObj = map.get(key);
        if(retObj != null){
            return retObj;
        }
        map.put(key, defaultValue);
        return defaultValue;
    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data){
        // 获取该 topic-queueId 下的最大位点 offset
        Map<Integer, Long> topicOffset = getOrPutDefault(appendOffset, topic, new HashMap<>());
        long offset = topicOffset.getOrDefault(queueId, 0L);
        // 更新最大位点
        topicOffset.put(queueId, offset+1);

        Map<Integer, Map<Long, ByteBuffer>> map1 = getOrPutDefault(appendData, topic, new HashMap<>());
        Map<Long, ByteBuffer> map2 = getOrPutDefault(map1, queueId, new HashMap<>());
        // 保存 data 中的数据
        ByteBuffer buf = ByteBuffer.allocate(data.remaining());
        buf.put(data);
        map2.put(offset, buf);
        return offset;
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        Map<Integer, ByteBuffer> ret = new HashMap<>();
        for(int i = 0; i < fetchNum; i++){
            Map<Integer, Map<Long, ByteBuffer>> map1 = appendData.get(topic);
            if(map1 == null){
                break;
            }
            Map<Long, ByteBuffer> m2 = map1.get(queueId);
            if(m2 == null){
                break;
            }
            ByteBuffer buf = m2.get(offset+i);
            if(buf != null){
                // 返回前确保 ByteBuffer 的 remain 区域为完整答案
                buf.position(0);
                buf.limit(buf.capacity());
                ret.put(i,buf);
            }
        }
        return ret;
    }
}
