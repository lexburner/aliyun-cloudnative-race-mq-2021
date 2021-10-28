package io.openmessaging;

/**
 * @author jingfeng.xjf
 * @date 2021/9/10
 *
 * 多级索引
 */
public class OffsetAndLen {

    private int length;

    private long ssdOffset;

    private long aepOffset;

    private long aepWriteBufferOffset;

    private int coldReadOffset;

    public OffsetAndLen() {
    }

    public OffsetAndLen(long ssdOffset, int length) {
        this.ssdOffset = ssdOffset;
        this.length = length;
        this.aepOffset = -1;
        this.coldReadOffset = -1;
        this.aepWriteBufferOffset = -1;
    }

    public long getSsdOffset() {
        return ssdOffset;
    }

    public void setSsdOffset(long ssdOffset) {
        this.ssdOffset = ssdOffset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public long getAepOffset() {
        return aepOffset;
    }

    public void setAepOffset(long aepOffset) {
        this.aepOffset = aepOffset;
    }

    public int getColdReadOffset() {
        return coldReadOffset;
    }

    public void setColdReadOffset(int coldReadOffset) {
        this.coldReadOffset = coldReadOffset;
    }

    public long getAepWriteBufferOffset() {
        return aepWriteBufferOffset;
    }

    public void setAepWriteBufferOffset(long aepWriteBufferOffset) {
        this.aepWriteBufferOffset = aepWriteBufferOffset;
    }
}
