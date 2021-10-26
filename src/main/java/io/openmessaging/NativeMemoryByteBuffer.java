package io.openmessaging;

import java.nio.ByteBuffer;

import sun.nio.ch.DirectBuffer;

/**
 * @author jingfeng.xjf
 * @date 2021/10/11
 */
public class NativeMemoryByteBuffer {

    private final ByteBuffer directBuffer;
    private final long address;
    private final int capacity;
    private int position;
    private int limit;

    public NativeMemoryByteBuffer(int capacity) {
        this.capacity = capacity;
        this.directBuffer = ByteBuffer.allocateDirect(capacity);
        this.address = ((DirectBuffer)directBuffer).address();
        this.position = 0;
        this.limit = capacity;
    }

    public int remaining() {
        return limit - position;
    }

    /**
     * 注意不会修改 heapBuffer 的 position 和 limit
     *
     * @param heapBuffer
     */
    public void put(ByteBuffer heapBuffer) {
        int remaining = heapBuffer.remaining();
        Util.unsafe.copyMemory(heapBuffer.array(), 16, null, address + position, remaining);
        position += remaining;
    }

    public void copyTo(ByteBuffer heapBuffer) {
        Util.unsafe.copyMemory(null, address + position, heapBuffer.array(), 16, limit);
    }

    public void put(byte b) {
        Util.unsafe.putByte(address + position, b);
        position++;
    }

    public void putShort(short b) {
        Util.unsafe.putShort(address + position, b);
        position += 2;
    }

    public byte get() {
        byte b = Util.unsafe.getByte(address + position);
        position++;
        return b;
    }

    public short getShort() {
        short s = Util.unsafe.getShort(address + position);
        position += 2;
        return s;
    }

    public ByteBuffer slice() {
        directBuffer.limit(limit);
        directBuffer.position(position);
        return directBuffer.slice();
    }

    public ByteBuffer duplicate() {
        return directBuffer;
    }

    public int position() {
        return position;
    }

    public void position(int position) {
        this.position = position;
    }

    public void limit(int limit) {
        this.limit = limit;
    }

    public void flip() {
        limit = position;
        position = 0;
    }

    public void clear() {
        position = 0;
        limit = capacity;
    }

}
