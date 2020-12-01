package com.zto.fire.demo.bean;

import com.google.common.collect.Maps;
import com.zto.fire.common.util.DateFormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 线程池中的线程中断方式，要使用线程池的submit方式才能正常中断
 * 而且需要submit Callable的子类完成cancel
 *
 * @author ChengLong 2019-5-11 13:29:51
 */
public class ThreadPoolInterrupt {
    public static Map<String, Task> taskMap = Maps.newConcurrentMap();
    private static final Logger logger = LoggerFactory.getLogger(ThreadPoolInterrupt.class);

    /**
     * 线程池中执行的线程子类
     */
    private static class Task implements Callable<String> {
        // 启动时间（s）
        private long time = System.currentTimeMillis();
        private int timeOut;
        private ExecutorService pool;
        private String taskName;
        private Future<String> future;

        public Task(ExecutorService pool, String taskName, int timeOut) {
            this.pool = pool;
            this.taskName = taskName;
            this.timeOut = timeOut;
            taskMap.put(this.taskName, this);
        }

        /**
         * 循环执行线程中的逻辑，模拟线程被阻塞
         *
         * @return
         * @throws Exception
         */
        @Override
        public String call() throws Exception {
            boolean flag = true;
            while (flag) {
                logger.info("--> {} 执行中 {}", this.taskName, DateFormatUtils.formatCurrentDateTime());
                Thread.sleep(1000);
            }
            return "执行成功";
        }

        /**
         * 提交当前线程到线程池中
         */
        public void submit() {
            this.future = this.pool.submit(this);
        }

        /**
         * 判断线程运行时间与超时时间，如果超时，则终止执行
         *
         * @return true : 执行中断操作  false: 不执行中断，不阻塞调用线程
         */
        public boolean stop() {
            if ((System.currentTimeMillis() - this.time) >= this.timeOut * 1000) {
                logger.info("关闭线程：{} 超时时间：{}s 运行时间：{}ms", this.taskName, this.timeOut, System.currentTimeMillis() - this.time);
                this.future.cancel(true);
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * 守护线程，用于轮询taskMap中所有的子线程执行时间是否超时
     */
    private static class DaemonThread implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    for (Map.Entry<String, Task> entry : taskMap.entrySet()) {
                        if (entry.getValue().stop()) {
                            taskMap.remove(entry.getKey());
                        }
                    }
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(5);
        new Task(pool, "task1", 10).submit();
        new Task(pool, "task2", 5).submit();
        new Task(pool, "task3", 20).submit();
        new Task(pool, "task4", 2).submit();
        pool.execute(new DaemonThread());

        Thread.currentThread().join();
    }
}
