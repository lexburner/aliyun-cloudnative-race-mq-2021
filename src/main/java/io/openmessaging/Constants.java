package io.openmessaging;

/**
 * @author jingfeng.xjf
 * @date 2021/9/10
 */
public class Constants {

    public static final String ESSD_BASE_PATH = "/essd";
    public static final String AEP_BASE_PATH = "/pmem";

    /**
     * topicId + queueId + offset + length
     */
    public static final int IDX_GROUP_BLOCK_SIZE = 1 + 2 + 2;

    /**
     * 每个data的大小为100B-17KiB
     */
    public static final int MAX_ONE_DATA_SIZE = 17 * 1024;

    /**
     * 评测程序会创建10~50个线程，实际性能评测会有 40 个线程，正确性评测少于 40
     */
    public static final int GROUPS = 4;

    /**
     * 性能评测的线程数
     */
    public static final int PERF_THREAD_NUM = 40;

    /**
     * 线程组聚合的消息数
     */
    public static final int NUMS_PERHAPS_IN_GROUP = 40 / GROUPS + 1;

    /**
     * 每个线程随机若干个topic（topic总数<=100）
     */
    public static final int TOPIC_NUM = 100;

    /**
     * 每个topic有N个queueId（1 <= N <= 5,000）
     */
    public static final int QUEUE_NUM = 5000;

    /**
     * getRange fetch num，题面是 100，实际最大是 30，节约内存
     */
    public static final int MAX_FETCH_NUM = 30;

    /**
     * 控制正确性检测的开关，用于调试
     */
    public static final boolean INTERRUPT_INCORRECT_PHASE = false;

    /**
     * 每个线程分配的 aep 滑动窗口缓存，用于热读
     */
    public static final long THREAD_AEP_HOT_CACHE_WINDOW_SIZE = 1598 * 1024 * 1024;

    /**
     * AEP 预分配的大小
     */
    public static final long THREAD_AEP_HOT_CACHE_PRE_ALLOCATE_SIZE = 1536 * 1024 * 1024;

    /**
     * 每个线程分配的 dram 写缓冲，用于缓存最热的数据
     * 50.6 * 1024
     */
    public static final int THREAD_AEP_WRITE_BUFFER_SIZE = 51814 * 1024;

    /**
     * 每个线程分配的 dram 冷读缓存，定长
      */
    public static final int THREAD_COLD_READ_BUFFER_SIZE = 80 * 1024 * 1024;

    /**
     * 判断冷数据是否应该缓存的阈值
     */
    public static final int THREAD_COLD_READ_THRESHOLD_SIZE = 8 * 1024;

    /**
     * 每个线程推测的数据量
     */
    public static final int THREAD_MSG_NUM = 385000;

    /**
     * ssd 预分配的文件大小
     */
    public static final long THREAD_GROUP_PRE_ALLOCATE_FILE_SIZE = 33L * 1024 * 1024 * 1024;

    public static int _4kb = 4 * 1024;

}
