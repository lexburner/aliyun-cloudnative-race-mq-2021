package io.openmessaging;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jingfeng.xjf
 * @date 2021/9/22
 *
 * 本赛题大多数操作都是 thread-bound 类的，利用 Context 来管理上下文
 */
public class Context {

    /**
     * 用于统计各个介质命中率的计数器
     */
    public static AtomicLong heapDram = new AtomicLong(0);
    public static AtomicLong ssd = new AtomicLong(0);
    public static AtomicLong aep = new AtomicLong(0);
    public static AtomicLong directDram = new AtomicLong(0);

    /**
     * 将所有线程分成 10 组
     */
    public static ThreadGroupManager[] threadGroupManagers = new ThreadGroupManager[Constants.GROUPS];

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
