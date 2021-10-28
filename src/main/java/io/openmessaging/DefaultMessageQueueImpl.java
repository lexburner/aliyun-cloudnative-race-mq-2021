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

/**
 * 程序的主入口
 */
public class DefaultMessageQueueImpl extends MessageQueue {

    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private final ThreadLocal<Boolean> firstAppend = ThreadLocal.withInitial(() -> true);

    private final AtomicInteger threadNum = new AtomicInteger(0);

    private final AtomicInteger messageNum = new AtomicInteger(0);

    public DefaultMessageQueueImpl() {

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

        for (int i = 0; i < Constants.GROUPS; i++) {
            Context.threadGroupManagers[i] = new ThreadGroupManager(i);
        }

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(Util::analysisMemory, 60, 10, TimeUnit.SECONDS);

        // 统计命中率
        Runtime.getRuntime().addShutdownHook(new Thread(this::analysisHitCache));

        // 防止程序卡死
        scheduledExecutorService.schedule(() -> {
            System.out.println("timeout killed");
            System.exit(0);
        }, 445, TimeUnit.SECONDS);

    }

    @Override
    public long append(String topic, int queueId, ByteBuffer data) {
        //        messageNum.incrementAndGet();
        // 统计线程总数
        if (firstAppend.get()) {
            initThread();
        }
        int topicNo = Util.parseInt(topic);

        // 获取该 topic-queueId 下的最大位点 offset
        return Context.threadGroupManager.get().append(topicNo, queueId, data);
    }

    @Override
    public Map<Integer, ByteBuffer> getRange(String topic, int queueId, long offset, int fetchNum) {
        int topicNo = Util.parseInt(topic);
        return Context.groupTopicManagers[topicNo].getRange(queueId, (int)offset, fetchNum);
    }

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

}