package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ConcurrentLinkedQueue;

public class OPageCacheByteBuffersPool {
  private final ByteBuffer ZERO_BUFFER;

  private static OPageCacheByteBuffersPool INSTANCE = new OPageCacheByteBuffersPool(
      OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024);

  private final ConcurrentLinkedQueue<ByteBuffer> buffersPool = new ConcurrentLinkedQueue<ByteBuffer>();
  private final int pageSize;

  public static OPageCacheByteBuffersPool instance() {
    return INSTANCE;
  }

  public OPageCacheByteBuffersPool(int pageSize) {
    this.pageSize = pageSize;
    ZERO_BUFFER = ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());
  }

  public ByteBuffer acquire() {
    return acquire(false);
  }

  public ByteBuffer acquire(boolean clearBuffer) {
    final ByteBuffer buffer = buffersPool.poll();
    if (buffer == null)
      return ByteBuffer.allocateDirect(pageSize).order(ByteOrder.nativeOrder());

    if (clearBuffer) {
      buffer.position(0);
      buffer.put(ZERO_BUFFER);
    }

    return buffer;
  }

  public void release(ByteBuffer byteBuffer) {
    assert byteBuffer != null;
    byteBuffer.rewind();
    buffersPool.offer(byteBuffer);
  }
}
