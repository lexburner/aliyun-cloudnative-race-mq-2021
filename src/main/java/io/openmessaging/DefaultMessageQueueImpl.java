package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultMessageQueueImpl extends MessageQueue {

    public DefaultMessageQueueImpl() {

        long start = System.currentTimeMillis();

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        Util.printParams();

        for (int i = 0; i < Constants.TOPIC_NUM; i++) {
            Context.groupTopicManagers[i] = new GroupTopicManager(i);
        }

        for (int i = 0; i < Constants.PERF_THREAD_NUM; i++) {
            Context.aepManagers[i] = new AepManager("hot_" + i,
                Constants.THREAD_AEP_WRITE_BUFFER_SIZE,
                Constants.THREAD_AEP_HOT_CACHE_WINDOW_SIZE);
            Context.fixedLengthDramManagers[i] = new FixedLengthDramManager(
                Constants.THREAD_COLD_READ_BUFFER_SIZE);
        }

        for (int i = 0; i < Constants.MAX_GROUPS; i++) {
            // 注意这里所有的 GroupManager 都持有相同的 groupTopicManagers 数组，以便于访问任意一个 topic
            Context.threadGroupManagers[i] = new ThreadGroupManager(i);
        }

        System.out.println("preallocate cost " + (System.currentTimeMillis() - start) + " ms");

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            Util.analysisMemory();
        }, 60, 10, TimeUnit.SECONDS);

        // 统计命中率
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("shutdown shook invoke");
            analysisHitCache();
        }));

        // 防止程序卡死
        scheduledExecutorService.schedule(() -> {
            System.out.println("timeout killed");
            System.exit(0);
        }, 445, TimeUnit.SECONDS);

        startTime = System.currentTimeMillis();
    }

    private void analysisHitCache() {
        long totalSize = 0;
        System.out.println(new Date() + " totalNum: " + messageNum.get() + " " + Util.byte2M(totalSize) + "M");
        System.out.println("=== message num: "
            + Context.heapDram.get() + " "
            + Context.ssd.get() + " "
            + Context.aep.get() + " "
            + Context.directDram.get()
        );
    }

    private final ThreadLocal<Boolean> firstAppend = ThreadLocal.withInitial(() -> true);

    private AtomicInteger threadNum = new AtomicInteger(0);

    private AtomicInteger messageNum = new AtomicInteger(0);

    long startTime;

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        //        messageNum.incrementAndGet();
        // 统计线程总数
        if (firstAppend.get()) {
            initThread();
        }
        int topicNo = Util.parseInt(topic);

        // 获取该 topic-queueId 下的最大位点 offset
        long offset = Context.threadGroupManager.get().append(topicNo, queueId, data);
        return offset;
    }

    Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

    private void initThread() {
        lock.lock();
        try {
            if (threadNum.incrementAndGet() != Constants.PERF_THREAD_NUM) {
                try {
                    boolean await = condition.await(3, TimeUnit.SECONDS);
                    if (!await && Constants.INTERRUPT_INCORRECT_PHASE) {
                        throw new RuntimeException("INTERRUPT_INCORRECT_PHASE");
                    }
                } catch (InterruptedException ignored) {
                }
            } else {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
        int groupNum;
        if (threadNum.get() == Constants.PERF_THREAD_NUM) {
            groupNum = Constants.GROUPS;
        } else {
            groupNum = threadNum.get();
        }
        firstAppend.set(false);
        int bucketNo = (int)(Thread.currentThread().getId() % groupNum);
        Context.threadGroupManager.set(Context.threadGroupManagers[bucketNo]);
        Context.threadGroupManagers[bucketNo].setTotal(threadNum.get() / groupNum);

    }

    //private void initThread() {
    //    threadNum.incrementAndGet();
    //    // 确保统计到所有线程
    //    try {
    //        Thread.sleep(100);
    //    } catch (InterruptedException e) {
    //        e.printStackTrace();
    //    }
    //    int groupNum = Constants.GROUPS;
    //    if (threadNum.get() != 40) {
    //        if (Constants.INTERRUPT_INCORRECT_PHASE) {
    //            throw new RuntimeException("INTERRUPT_INCORRECT_PHASE");
    //        }
    //        groupNum = threadNum.get();
    //    }
    //    firstAppend.set(false);
    //    int bucketNo = (int)(Thread.currentThread().getId() % groupNum);
    //    Context.threadGroupManager.set(Context.threadGroupManagers[bucketNo]);
    //    Context.threadGroupManagers[bucketNo].joinGroup();
    //    // 确保所有线程都加入了 group
    //    try {
    //        Thread.sleep(100);
    //    } catch (InterruptedException e) {
    //        e.printStackTrace();
    //    }
    //}

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        int topicNo = Util.parseInt(topic);

        Map<Integer, ByteBuffer> ret = Context.groupTopicManagers[topicNo].getRange(queueId, (int)offset, fetchNum);

        return ret;
    }

}