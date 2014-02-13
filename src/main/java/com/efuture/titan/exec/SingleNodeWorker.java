
package com.efuture.titan.exec;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.efuture.titan.common.conf.TitanConf;
//import com.efuture.titan.net.bio.Connection;
import com.efuture.titan.net.bio.ConnectionPool;
import com.efuture.titan.session.SessionState;

public abstract class SingleNodeWorker implements NodeWorker {

  protected TitanConf conf;
  protected SessionState ss;
  protected QueryPlan plan;
  protected boolean isAutoCommit;

  protected AtomicBoolean isRunning = new AtomicBoolean(false);
  protected AtomicBoolean isFailed = new AtomicBoolean(false);
  protected final ReentrantLock lock = new ReentrantLock();
  protected final Condition taskFinished = lock.newCondition();

  public SingleNodeWorker(TitanConf conf, SessionState ss) {
    this.conf = conf;
    this.ss = ss;
  }

  public void execute(QueryPlan plan) {
    this.plan = plan;
    this.isAutoCommit = plan.isAutoCommit();
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      isRunning.set(true);
    } finally {
      lock.unlock();
    }

    // check frontend connection
    if (ss.getFrontendConnection().isClosed()) {
      endRunning();
      return;
    } 

    DataNodeTask task = plan.getDataNodeTasks().get(0);

    // Single node, single connection
    final Connection conn = ConnectionPool.newConnection(ss,
        task.getDataNode(), task.getReplicaIndex());
    final String sql = task.getSQL();
    ExecuteThreadPool.get(conf).execute(new Runnable() {
      @Override
      public void run() {
        _execute(conn, sql);
      }
    });

  }

  abstract protected void _execute(Connection conn, String sql);

  @Override
  public void commit() {
    DataNodeTask task = plan.getDataNodeTasks().get(0);
    final Connection conn = ConnectionPool.getConnection(ss,
        task.getDataNode(), task.getReplicaIndex());

    // 初始化
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      isRunning.set(true);
      isFailed.set(false);
    } finally {
      lock.unlock();
    }

    if (ss.getFrontendConnection().isClosed()) {
      endRunning();
      return;
    }

    ExecuteThreadPool.get(conf).execute(new Runnable() {
      @Override
      public void run() {
        _commit(conn);
      }
    });

  }

  abstract protected void _commit(Connection conn);

  @Override
  public void rollback() {
    DataNodeTask task = plan.getDataNodeTasks().get(0);
    final Connection conn = ConnectionPool.getConnection(ss,
        task.getDataNode(), task.getReplicaIndex());

    // 初始化
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      isRunning.set(true);
      isFailed.set(false);
    } finally {
      lock.unlock();
    }

    if (ss.getFrontendConnection().isClosed()) {
      endRunning();
      return;
    }

    ExecuteThreadPool.get(conf).execute(new Runnable() {
      @Override
      public void run() {
        _rollback(conn);
      }
    });
  }

  abstract protected void _rollback(Connection conn);

  protected void endRunning() {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      isRunning.set(false);
      taskFinished.signalAll();
    } finally {
      lock.unlock();
    }
  }

  private void clear() {
    ConnectionPool.clear(ss);
  }

  @Override
  public void terminate() throws InterruptedException {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      while (isRunning.get()) {
        taskFinished.await();
      }
    } finally {
      lock.unlock();
    }
  }
}
