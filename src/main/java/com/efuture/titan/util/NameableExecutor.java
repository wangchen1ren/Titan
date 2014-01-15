
package com.efuture.titan.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class NameableExecutor extends ThreadPoolExecutor {
  protected String name;

  public static NameableExecutor newExecutor(String name, int poolSize) {
    return newExecutor(name, poolSize, true);
  }

  public static NameableExecutor newExecutor(String name, int poolSize, boolean isDaemon) {
    ThreadFactory factory = new ThreadFactoryBuilder()
        .setNameFormat(name + "-%d")
        .setPriority(Thread.NORM_PRIORITY)
        .setDaemon(true)
        .build();
    return new NameableExecutor(name, poolSize, new LinkedBlockingQueue<Runnable>(), factory);
  }

  public NameableExecutor(String name, int poolSize, BlockingQueue<Runnable> queue, ThreadFactory factory) {
    super(poolSize, poolSize, Long.MAX_VALUE, TimeUnit.NANOSECONDS, queue, factory);
    this.name = name;
  }   

  public String getName() {
    return name;
  }   

}
