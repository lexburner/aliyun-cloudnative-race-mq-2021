package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author jingfeng.xjf
 * @date 2021/9/16
 *
 * HashMap 的数组实现版本
 */
public class ArrayMap implements Map<Integer, ByteBuffer> {

    private int capacity;
    private int size;
    private ByteBuffer[] byteBuffer;

    public ArrayMap() {
    }

    public ArrayMap(int capacity) {
        this.capacity = capacity;
        this.byteBuffer = new ByteBuffer[capacity];
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return (Integer)key < this.size;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer get(Object key) {
        int index = (Integer)key;
        if (index < size) {
            return byteBuffer[index];
        }
        return null;
    }

    @Override
    public synchronized ByteBuffer put(Integer key, ByteBuffer value) {
        byteBuffer[key] = value;
        size++;
        return value;
    }

    @Override
    public ByteBuffer remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends Integer, ? extends ByteBuffer> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        this.size = 0;
    }

    @Override
    public Set<Integer> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<ByteBuffer> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<Integer, ByteBuffer>> entrySet() {
        throw new UnsupportedOperationException();
    }
}
