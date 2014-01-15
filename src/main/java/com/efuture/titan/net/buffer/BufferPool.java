
package com.efuture.titan.net.buffer;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.LinkedList;

public class BufferPool {
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024 * 64;
  private static final int DEFAULT_BUFFER_CHUNK_SIZE = 4096;

  private static BufferPool instance;

  private int size;
  private int chunkSize;
  private LinkedList<ByteBuffer> chunks;

  public static BufferPool getInstance() {
    if (instance == null) {
      instance = new BufferPool();
    }
    return instance;
  }

  public BufferPool() {
    this(DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_CHUNK_SIZE);
  }

  public BufferPool(int bufferSize, int chunkSize) {
    this.chunkSize = chunkSize;
    chunks = new LinkedList<ByteBuffer>();
    size = bufferSize / chunkSize + 1;
    for (int i = 0; i < size; i++) {
      chunks.offer(create(chunkSize));
    }
  }

  public synchronized ByteBuffer allocate() {
    ByteBuffer node = chunks.poll();
    if (node == null) {
      node = create(chunkSize);
    }
    return node;
  }

  public synchronized void recycle(ByteBuffer buffer) {
    // 拒绝回收null和容量大于chunkSize的缓存
    if (buffer == null || buffer.capacity() > chunkSize) {
      return;
    }
    if (chunks.size() < size) { 
        buffer.clear();
        chunks.offer(buffer);
    }
  }

  private ByteBuffer create(int size) {
    return ByteBuffer.allocate(size);
    //return ByteBuffer.allocateDirect(size); // for performance
  }

}
