
package com.efuture.titan.exec;

import com.efuture.titan.common.conf.TitanConf;
import com.efuture.titan.session.SessionState;

public abstract class SingleNodeWorker implements NodeWorker {

  protected TitanConf conf;
  protected SessionState ss;
  protected QueryPlan plan;
  protected boolean isAutoCommit;

  private AtomicBoolean isRunning = new AtomicBoolean(false);
  private AtomicBoolean isFailed = new AtomicBoolean(false);
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition taskFinished = lock.newCondition();

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
    ExecuteThreadPool.get().execute(new Runnable() {
      @Override
      public void run() {
        _execute(conn, sql);
      }
    });

  }

  abstract public void _execute(Connection conn, String sql);

  @Override
  public void commit() {
    DataNodeTask task = plan.getDataNodeTasks().get(0);
    final Connection conn = ConnectionPool.get(ss,
        task.getDataNode(), task.getReplicaIndex());
    if (conn == null) {
      ss.getFrontendConnection().write(OkPacket.OK);
    }

    // 初始化
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      isRunning.set(true);
      isFail.set(false);
    } finally {
      lock.unlock();
    }

    if (ss.getFrontendConnection().isClosed()) {
      endRunning();
      return;
    }

    ExecuteThreadPool.get().execute(new Runnable() {
      @Override
      public void run() {
        _commit(conn);
      }
    });

  }

  @Override
  public void rollback() {
    DataNodeTask task = plan.getDataNodeTasks().get(0);
    final Connection conn = ConnectionPool.get(ss,
        task.getDataNode(), task.getReplicaIndex());
    if (conn == null) {
      ss.getFrontendConnection().write(OkPacket.OK);
      return;
    }

    // 初始化
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      isRunning.set(true);
      isFail.set(false);
    } finally {
      lock.unlock();
    }

    if (ss.getFrontendConnection().isClosed()) {
      endRunning();
      return;
    }

    ExecuteThreadPool.get().execute(new Runnable() {
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
