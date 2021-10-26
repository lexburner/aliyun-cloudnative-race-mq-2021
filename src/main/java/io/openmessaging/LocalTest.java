package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * @author jingfeng.xjf
 * @date 2021/9/10
 */
public class LocalTest {

    public static void main(String[] args) {
        MessageQueue messageQueue = new DefaultMessageQueueImpl();
        ByteBuffer v1 = ByteBuffer.allocate(4);
        v1.putInt(1); v1.flip();
        ByteBuffer v2 = ByteBuffer.allocate(4);
        v2.putInt(2); v2.flip();
        ByteBuffer v3 = ByteBuffer.allocate(4);
        v3.putInt(3); v3.flip();
        ByteBuffer v4 = ByteBuffer.allocate(4);
        v4.putInt(4); v4.flip();
        messageQueue.append("topic1",1,v1);
        messageQueue.append("topic1",1,v2);
        messageQueue.append("topic1",1,v3);
        messageQueue.append("topic1",1,v4);

        Map<Integer, ByteBuffer> vals = messageQueue.getRange("topic1", 1, 0, 4);
        System.out.println(vals.get(0).getInt());
        System.out.println(vals.get(1).getInt());
        System.out.println(vals.get(2).getInt());
        System.out.println(vals.get(3).getInt());
    }

}
