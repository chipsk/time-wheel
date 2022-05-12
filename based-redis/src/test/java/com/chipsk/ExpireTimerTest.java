package com.chipsk;

import com.chipsk.jedis.JedisUtil;
import com.chipsk.timer.Task;
import com.chipsk.timer.expire.ExpireTimer;

import com.chipsk.timer.expire.RedisMsgPubSubListener;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ExpireTimerTest {
    @Test
    @SuppressWarnings("serial")
    public void testTimer() throws Exception {
        ExpireTimer timer = new ExpireTimer();
        final SimpleDateFormat s = new SimpleDateFormat("hh:mm:ss:SSS");
        System.out.println(s.format(new Date()) + "提交了！");
        for (int i = 0; i < 3; i++) {
            timer.addTask(new Task() {

                @Override
                public void run() {
                    System.out.println(s.format(new Date()) + "执行了！");
                }
            }, 5, TimeUnit.SECONDS);
        }

        timer.addTask(new Task() {

            @Override
            public void run() {
                System.out.println(s.format(new Date()) + "执行了！");
            }
        }, 100, TimeUnit.SECONDS);
        Thread.sleep(7000);
        System.out.println(timer.stop().size());
    }

    @Test
    public void testPubSub() throws InterruptedException {
        JedisUtil.getJedis().subscribe(new RedisMsgPubSubListener(null), "__keyevent@0__:expired");
    }
}
