
package com.efuture.titan.net.buffer;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class BufferQueue {
  public static final int NEARLY_FULL = -1;
  public static final int NEARLY_EMPTY = 1;

  private LinkedList<ByteBuffer> chunks;
  private ByteBuffer attachment;
  private int capacity;

  public BufferQueue(int capacity) {
    this.capacity = capacity;
    chunks = new LinkedList<ByteBuffer>();
  }

  public ByteBuffer attachment() {
    return attachment;
  }

  public void attach(ByteBuffer buffer) {
    this.attachment = buffer;
  }

  public synchronized int size() {
    return chunks.size();
  }

  public int getStatus() {
    int size = chunks.size();
    if (size > (capacity - 2)) {
      return NEARLY_FULL;
    } else if (size < capacity * 1 / 3) {
      return NEARLY_EMPTY;
    }
    return 0;
  }

  public synchronized boolean offer(ByteBuffer buffer)
      throws InterruptedException {
    return chunks.offer(buffer);
  }

  public synchronized ByteBuffer poll() {
    return chunks.poll();
  }
}
