/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Base page class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * <p>
 * This page has several booked memory areas with following offsets at the beginning:
 * <ol>
 * <li>from 0 to 7 - Magic number</li>
 * <li>from 8 to 11 - crc32 of all page content, which is calculated by cache system just before save</li>
 * <li>from 12 to 23 - LSN of last operation which was stored for given page</li>
 * </ol>
 * <p>
 * Developer which will extend this class should use all page memory starting from {@link #NEXT_FREE_POSITION} offset.
 * <p>
 * {@link OReadCache#release(OCacheEntry, com.orientechnologies.orient.core.storage.cache.OWriteCache)} back to the cache.
 * <p>
 * All data structures which use this kind of pages should be derived from
 * {@link com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} class.
 *
 * @author Andrey Lomakin
 * @since 16.08.13
 */
public class ODurablePage {
  protected static final int MAGIC_NUMBER_OFFSET = 0;
  protected static final int CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int MAX_PAGE_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  protected static final int NEXT_FREE_POSITION = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  protected OWALChangesTree changesTree;

  private final OCacheEntry cacheEntry;
  private final ByteBuffer  byteBuffer;

  public ODurablePage(OCacheEntry cacheEntry, OWALChangesTree changesTree) {
    assert cacheEntry != null || changesTree != null;

    this.cacheEntry = cacheEntry;

    if (cacheEntry != null) {
      final OCachePointer cachePointer = cacheEntry.getCachePointer();
      this.byteBuffer = cachePointer.getByteBuffer();
    } else
      this.byteBuffer = null;

    this.changesTree = changesTree;
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(ByteBuffer byteBuffer) {
    final long segment = OLongSerializer.INSTANCE.deserializeFromByteBufferObject(byteBuffer, WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeFromByteBufferObject(byteBuffer, WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public static void getPageData(ByteBuffer byteBuffer, byte[] data, int offset, int length) {
    byteBuffer.position(0);
    byteBuffer.get(data, offset, length);
  }

  public static OLogSequenceNumber getLogSequenceNumber(int offset, byte[] data) {
    final long segment = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_SEGMENT_OFFSET);
    final long position = OLongSerializer.INSTANCE.deserializeNative(data, offset + WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  protected int getIntValue(int pageOffset) {
    if (changesTree == null)
      return OIntegerSerializer.INSTANCE.deserializeFromByteBufferObject(byteBuffer, pageOffset);

    return OIntegerSerializer.INSTANCE.deserializeFromByteBufferObject(changesTree.wrap(byteBuffer), pageOffset);
  }

  protected long getLongValue(int pageOffset) {
    if (changesTree == null)
      return OLongSerializer.INSTANCE.deserializeFromByteBufferObject(byteBuffer, pageOffset);

    return OLongSerializer.INSTANCE.deserializeFromByteBufferObject(changesTree.wrap(byteBuffer), pageOffset);
  }

  protected byte[] getBinaryValue(int pageOffset, int valLen) {
    if (changesTree == null) {
      final byte[] res = new byte[valLen];
      byteBuffer.position(pageOffset);
      byteBuffer.get(res);
      return res;
    }

    return changesTree.getBinaryValue(byteBuffer, pageOffset, valLen);
  }

  protected int getObjectSizeInByteBuffer(OBinarySerializer binarySerializer, int offset) {
    if (changesTree == null)
      return binarySerializer.getObjectSizeInByteBuffer(byteBuffer, offset);

    return binarySerializer.getObjectSizeInByteBuffer(changesTree.wrap(byteBuffer), offset);
  }

  protected <T> T deserializeFromDirectMemory(OBinarySerializer<T> binarySerializer, int offset) {
    if (changesTree == null)
      return binarySerializer.deserializeFromByteBufferObject(byteBuffer, offset);

    return binarySerializer.deserializeFromByteBufferObject(changesTree.wrap(byteBuffer), offset);
  }

  protected byte getByteValue(int pageOffset) {
    if (changesTree == null)
      return byteBuffer.get(pageOffset);

    return changesTree.getByteValue(byteBuffer, pageOffset);
  }

  protected int setIntValue(int pageOffset, int value) throws IOException {
    if (changesTree != null) {
      byte[] svalue = new byte[OIntegerSerializer.INT_SIZE];
      OIntegerSerializer.INSTANCE.serializeNative(value, svalue, 0);

      changesTree.add(svalue, pageOffset);
    } else
      OIntegerSerializer.INSTANCE.serializeInByteBuffer(value, byteBuffer, pageOffset);

    cacheEntry.markDirty();

    return OIntegerSerializer.INT_SIZE;

  }

  protected int setByteValue(int pageOffset, byte value) {
    if (changesTree != null) {
      changesTree.add(new byte[] { value }, pageOffset);
    } else
      byteBuffer.put(pageOffset, value);

    cacheEntry.markDirty();

    return OByteSerializer.BYTE_SIZE;
  }

  protected int setLongValue(int pageOffset, long value) throws IOException {
    if (changesTree != null) {
      byte[] svalue = new byte[OLongSerializer.LONG_SIZE];
      OLongSerializer.INSTANCE.serializeNative(value, svalue, 0);

      changesTree.add(svalue, pageOffset);
    } else
      OLongSerializer.INSTANCE.serializeInByteBuffer(value, byteBuffer, pageOffset);

    cacheEntry.markDirty();

    return OLongSerializer.LONG_SIZE;
  }

  protected int setBinaryValue(int pageOffset, byte[] value) throws IOException {
    if (value.length == 0)
      return 0;

    if (changesTree != null) {
      changesTree.add(value, pageOffset);
    } else {
      byteBuffer.position(pageOffset);
      byteBuffer.put(value, 0, value.length);
    }

    cacheEntry.markDirty();

    return value.length;
  }

  protected void moveData(int from, int to, int len) throws IOException {
    if (len == 0)
      return;

    if (changesTree != null) {
      byte[] content = changesTree.getBinaryValue(byteBuffer, from, len);

      changesTree.add(content, to);
    } else {
      final ByteBuffer clone = byteBuffer.asReadOnlyBuffer();
      clone.position(from);
      clone.limit(from + len);

      byteBuffer.position(to);
      byteBuffer.put(clone);
    }

    cacheEntry.markDirty();
  }

  public OWALChangesTree getChangesTree() {
    return changesTree;
  }

  public void restoreChanges(OWALChangesTree changesTree) {
    changesTree.applyChanges(cacheEntry.getCachePointer().getByteBuffer());
    cacheEntry.markDirty();
  }

  public OLogSequenceNumber getLsn() {
    final long segment = getLongValue(WAL_SEGMENT_OFFSET);
    final long position = getLongValue(WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public void setLsn(OLogSequenceNumber lsn) {
    OLongSerializer.INSTANCE.serializeInByteBuffer(lsn.getSegment(), byteBuffer, WAL_SEGMENT_OFFSET);
    OLongSerializer.INSTANCE.serializeInByteBuffer(lsn.getPosition(), byteBuffer, WAL_POSITION_OFFSET);

    cacheEntry.markDirty();
  }
}
