package com.hmdp.utils;

public interface ILock {

    /**
     * 尝试获取锁
     * @param timeSec
     * @return
     */
    boolean tryLock(long timeSec);

    /**
     * 释放锁
     */
    void unLock();
}
