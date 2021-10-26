package io.openmessaging;

import java.nio.ByteBuffer;

/**
 * @author jingfeng.xjf
 * @date 2021/9/29
 */
public class FixedLengthDramManager {

    private final ByteBuffer buffer;

    public FixedLengthDramManager(int buffer) {
        this.buffer = ByteBuffer.allocate(buffer);
    }

    public int write(ByteBuffer data) {
        int offset = -1;
        if (
            data.remaining() <= Constants.THREAD_COLD_READ_THRESHOLD_SIZE &&
                data.remaining() <= this.buffer.remaining()) {
            offset = this.buffer.position();
            buffer.put(data);
            data.flip();
        }

        return offset;
    }

    public ByteBuffer read(int position, int len) {
        ByteBuffer duplicate = this.buffer.duplicate();
        duplicate.position(position);
        duplicate.limit(position + len);
        return duplicate.slice();
    }

}
