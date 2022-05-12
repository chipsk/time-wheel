package com.chipsk.timer;

import java.io.Serializable;

public interface ExceptionHandler extends Serializable{
    void handle(Task task, Throwable e);
}
