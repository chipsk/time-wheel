package com.chipsk;


import com.chipsk.timer.Task;
import com.chipsk.timer.zset.ZSetTimer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Client {
    public static void main(String[] args) throws InterruptedException {
        ZSetTimer rt = new ZSetTimer();
        final SimpleDateFormat s = new SimpleDateFormat("hh:mm:ss:SSS");
        for (int i = 0; i < 2; i++) {
            System.out.println(s.format(new Date()) + "提交任务！");
            rt.addTask(new Task() {

                @Override
                public void run() {
                    System.out.println(s.format(new Date()) + "执行了！");
                }
            }, 10, TimeUnit.SECONDS);
        }

        TimeUnit.SECONDS.sleep(12);

        rt.stop();
    }
}
