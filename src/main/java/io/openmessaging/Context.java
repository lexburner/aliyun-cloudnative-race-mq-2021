package io.openmessaging;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jingfeng.xjf
 * @date 2021/9/22
 */
public class Context {

    public static AtomicLong heapDram = new AtomicLong(0);
    public static AtomicLong ssd = new AtomicLong(0);
    public static AtomicLong aep = new AtomicLong(0);
    public static AtomicLong directDram = new AtomicLong(0);

    public static AtomicLong coldCacheSize = new AtomicLong(0);
    public static AtomicLong ssdSize = new AtomicLong(0);
    /**
     * 未被写入 aep
     */
    public static AtomicLong _0_ssdSize = new AtomicLong(0);
    public static AtomicLong _0_15_ssdSize = new AtomicLong(0);
    public static AtomicLong _15_30_ssdSize = new AtomicLong(0);
    public static AtomicLong _30_45_ssdSize = new AtomicLong(0);
    public static AtomicLong _45_75_ssdSize = new AtomicLong(0);
    public static AtomicLong _75_125_ssdSize = new AtomicLong(0);
    public static AtomicLong aepSize = new AtomicLong(0);
    public static AtomicLong dramSize = new AtomicLong(0);

    /**
     * 将所有线程分成 10 组
     */
    public static ThreadGroupManager[] threadGroupManagers = new ThreadGroupManager[Constants.MAX_GROUPS];
    /**
     * topic 级别的操作封装
     */
    public static GroupTopicManager[] groupTopicManagers = new GroupTopicManager[Constants.TOPIC_NUM];
    public static AepManager[] aepManagers = new AepManager[Constants.PERF_THREAD_NUM];
    public static FixedLengthDramManager[] fixedLengthDramManagers = new FixedLengthDramManager[Constants.PERF_THREAD_NUM];

    public static ThreadLocal<ThreadGroupManager> threadGroupManager = new ThreadLocal<>();
    public static ThreadLocal<AepManager> threadAepManager = ThreadLocal.withInitial(() -> aepManagers[(int) Thread.currentThread().getId() % Constants.PERF_THREAD_NUM]);
    public static ThreadLocal<FixedLengthDramManager> threadFixedLengthDramManager = ThreadLocal.withInitial(() -> fixedLengthDramManagers[(int) Thread.currentThread().getId() % Constants.PERF_THREAD_NUM]);

}
