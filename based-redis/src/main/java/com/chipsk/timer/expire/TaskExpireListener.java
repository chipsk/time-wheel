package com.chipsk.timer.expire;

public interface TaskExpireListener {
    void process(String taskId);
}
