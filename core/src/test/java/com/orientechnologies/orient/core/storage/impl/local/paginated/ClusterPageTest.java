package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import com.orientechnologies.common.util.MersenneTwisterFast;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OPageCacheByteBuffersPool;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin
 * @since 20.03.13
 */
@Test
public class ClusterPageTest {
  private static final int SYSTEM_OFFSET = 24;

  public void testAddOneRecord() throws Exception {
    OPageCacheByteBuffersPool pageCacheByteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(pageCacheByteBuffersPool, pageCacheByteBuffersPool.acquire(),
        new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(pageCacheByteBuffersPool, pageCacheByteBuffersPool.acquire(),
        new OLogSequenceNumber(0, 0), 0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      addOneRecord(localPage);
      addOneRecord(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), pageCacheByteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addOneRecord(OClusterPage localPage) throws IOException {
    int freeSpace = localPage.getFreeSpace();
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 1;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordsCount(), 1);
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (27 + ORecordVersionHelper.SERIALIZED_SIZE));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertEquals(localPage.getRecordBinaryValue(0, 0, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
  }

  public void testAddThreeRecords() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      addThreeRecords(localPage);
      addThreeRecords(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addThreeRecords(OClusterPage localPage) throws IOException {
    int freeSpace = localPage.getFreeSpace();

    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });

    Assert.assertEquals(localPage.getRecordsCount(), 3);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);

    Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (3 * (27 + ORecordVersionHelper.SERIALIZED_SIZE)));
    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));

    Assert.assertEquals(localPage.getRecordBinaryValue(0, 0, 11), new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertEquals(localPage.getRecordBinaryValue(1, 0, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    Assert.assertEquals(localPage.getRecordBinaryValue(2, 0, 11), new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    Assert.assertEquals(localPage.getRecordSize(0), 11);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);
  }

  public void testAddFullPage() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      addFullPage(localPage);
      addFullPage(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addFullPage(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;

    List<Integer> positions = new ArrayList<Integer>();
    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positions.size());

    counter = 0;
    for (int position : positions) {
      Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, 3), new byte[] { counter, counter, counter });
      Assert.assertEquals(localPage.getRecordSize(position), 3);
      Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
      counter++;
    }
  }

  public void testDeleteAddLowerVersion() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      deleteAddLowerVersion(localPage);
      deleteAddLowerVersion(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddLowerVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  public void testDeleteAddBiggerVersion() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      deleteAddBiggerVersion(localPage);
      deleteAddBiggerVersion(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddBiggerVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    int newRecordVersion = 0;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;
    newRecordVersion++;

    Assert.assertEquals(localPage.appendRecord(newRecordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), newRecordVersion);
    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  public void testDeleteAddEqualVersion() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      deleteAddEqualVersion(localPage);
      deleteAddEqualVersion(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  public void testDeleteAddEqualVersionKeepTombstoneVersion() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      deleteAddEqualVersionKeepTombstoneVersion(localPage);
      deleteAddEqualVersionKeepTombstoneVersion(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteAddEqualVersionKeepTombstoneVersion(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    int position = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });

    Assert.assertTrue(localPage.deleteRecord(position));

    Assert.assertEquals(localPage.appendRecord(recordVersion, new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 }), position);

    int recordSize = localPage.getRecordSize(position);
    Assert.assertEquals(recordSize, 11);

    Assert.assertEquals(localPage.getRecordVersion(position), recordVersion);
    Assert.assertEquals(localPage.getRecordBinaryValue(position, 0, recordSize), new byte[] { 2, 2, 2, 4, 5, 6, 5, 4, 2, 2, 2 });
  }

  public void testDeleteTwoOutOfFour() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      deleteTwoOutOfFour(localPage);
      deleteTwoOutOfFour(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void deleteTwoOutOfFour(OClusterPage localPage) throws IOException {
    int recordVersion = 0;
    recordVersion++;

    int positionOne = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int positionTwo = localPage.appendRecord(recordVersion, new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    int positionThree = localPage.appendRecord(recordVersion, new byte[] { 3, 2, 3, 4, 5, 6, 5, 4, 3, 2, 3 });
    int positionFour = localPage.appendRecord(recordVersion, new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });

    Assert.assertEquals(localPage.getRecordsCount(), 4);
    Assert.assertEquals(positionOne, 0);
    Assert.assertEquals(positionTwo, 1);
    Assert.assertEquals(positionThree, 2);
    Assert.assertEquals(positionFour, 3);

    Assert.assertFalse(localPage.isDeleted(0));
    Assert.assertFalse(localPage.isDeleted(1));
    Assert.assertFalse(localPage.isDeleted(2));
    Assert.assertFalse(localPage.isDeleted(3));

    int freeSpace = localPage.getFreeSpace();

    Assert.assertTrue(localPage.deleteRecord(0));
    Assert.assertTrue(localPage.deleteRecord(2));

    Assert.assertFalse(localPage.deleteRecord(0));
    Assert.assertFalse(localPage.deleteRecord(7));

    Assert.assertEquals(localPage.findFirstDeletedRecord(0), 0);
    Assert.assertEquals(localPage.findFirstDeletedRecord(1), 2);
    Assert.assertEquals(localPage.findFirstDeletedRecord(3), -1);

    Assert.assertTrue(localPage.isDeleted(0));
    Assert.assertEquals(localPage.getRecordSize(0), -1);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertEquals(localPage.getRecordBinaryValue(1, 0, 11), new byte[] { 2, 2, 3, 4, 5, 6, 5, 4, 3, 2, 2 });
    Assert.assertEquals(localPage.getRecordSize(1), 11);
    Assert.assertEquals(localPage.getRecordVersion(1), recordVersion);

    Assert.assertTrue(localPage.isDeleted(2));
    Assert.assertEquals(localPage.getRecordSize(2), -1);
    Assert.assertEquals(localPage.getRecordVersion(2), recordVersion);

    Assert.assertEquals(localPage.getRecordBinaryValue(3, 0, 11), new byte[] { 4, 2, 3, 4, 5, 6, 5, 4, 3, 2, 4 });
    Assert.assertEquals(localPage.getRecordSize(3), 11);
    Assert.assertEquals(localPage.getRecordVersion(3), recordVersion);

    Assert.assertEquals(localPage.getRecordsCount(), 2);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace + 23 * 2);
  }

  public void testAddFullPageDeleteAndAddAgain() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      addFullPageDeleteAndAddAgain(localPage);
      addFullPageDeleteAndAddAgain(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addFullPageDeleteAndAddAgain(OClusterPage localPage) throws IOException {
    Map<Integer, Byte> positionCounter = new HashMap<Integer, Byte>();
    Set<Integer> deletedPositions = new HashSet<Integer>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();
    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positionCounter.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i += 2) {
      localPage.deleteRecord(i);
      deletedPositions.add(i);
      positionCounter.remove(i);
    }

    freeSpace = localPage.getFreeSpace();
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        positionCounter.put(lastPosition, counter);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      Assert.assertEquals(localPage.getRecordBinaryValue(entry.getKey(), 0, 3),
          new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);

      if (deletedPositions.contains(entry.getKey()))
        Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);

    }
  }

  public void testAddBigRecordDeleteAndAddSmallRecords() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);

    try {
      final long seed = System.currentTimeMillis();

      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      addBigRecordDeleteAndAddSmallRecords(seed, localPage);
      addBigRecordDeleteAndAddSmallRecords(seed, directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void addBigRecordDeleteAndAddSmallRecords(long seed, OClusterPage localPage) throws IOException {
    final MersenneTwisterFast mersenneTwisterFast = new MersenneTwisterFast(seed);

    int recordVersion = 0;
    recordVersion++;
    recordVersion++;

    final byte[] bigChunk = new byte[OClusterPage.MAX_ENTRY_SIZE / 2];

    mersenneTwisterFast.nextBytes(bigChunk);

    int position = localPage.appendRecord(recordVersion, bigChunk);
    Assert.assertEquals(position, 0);
    Assert.assertEquals(localPage.getRecordVersion(0), recordVersion);

    Assert.assertTrue(localPage.deleteRecord(0));

    recordVersion++;
    int freeSpace = localPage.getFreeSpace();
    Map<Integer, Byte> positionCounter = new HashMap<Integer, Byte>();
    int lastPosition;
    byte counter = 0;
    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positionCounter.size());
        positionCounter.put(lastPosition, counter);
        counter++;

        if (lastPosition == 0)
          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - 15);
        else
          Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));

        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    Assert.assertEquals(localPage.getRecordsCount(), positionCounter.size());
    for (Map.Entry<Integer, Byte> entry : positionCounter.entrySet()) {
      Assert.assertEquals(localPage.getRecordBinaryValue(entry.getKey(), 0, 3),
          new byte[] { entry.getValue(), entry.getValue(), entry.getValue() });
      Assert.assertEquals(localPage.getRecordSize(entry.getKey()), 3);
      Assert.assertEquals(localPage.getRecordVersion(entry.getKey()), recordVersion);
    }
  }

  public void testFindFirstRecord() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    ODirectMemoryPointer directPagePointer = ODirectMemoryPointerFactory.instance().createPointer(new byte[OClusterPage.PAGE_SIZE]);
    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      findFirstRecord(seed, localPage);
      findFirstRecord(seed, directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void findFirstRecord(long seed, OClusterPage localPage) throws IOException {
    final MersenneTwisterFast mersenneTwister = new MersenneTwisterFast(seed);
    Set<Integer> positions = new HashSet<Integer>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = 0;
    int lastRecordPosition = -1;

    do {
      recordPosition = localPage.findFirstRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition > lastRecordPosition);

      lastRecordPosition = recordPosition;

      recordPosition++;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  public void testFindLastRecord() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);

    final long seed = System.currentTimeMillis();
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      findLastRecord(seed, localPage);
      findLastRecord(seed, directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void findLastRecord(long seed, OClusterPage localPage) throws IOException {
    final MersenneTwisterFast mersenneTwister = new MersenneTwisterFast(seed);
    Set<Integer> positions = new HashSet<Integer>();

    int lastPosition;
    byte counter = 0;
    int freeSpace = localPage.getFreeSpace();

    int recordVersion = 0;
    recordVersion++;

    do {
      lastPosition = localPage.appendRecord(recordVersion, new byte[] { counter, counter, counter });
      if (lastPosition >= 0) {
        Assert.assertEquals(lastPosition, positions.size());
        positions.add(lastPosition);
        counter++;

        Assert.assertEquals(localPage.getFreeSpace(), freeSpace - (19 + ORecordVersionHelper.SERIALIZED_SIZE));
        freeSpace = localPage.getFreeSpace();
      }
    } while (lastPosition >= 0);

    int filledRecordsCount = positions.size();
    Assert.assertEquals(localPage.getRecordsCount(), filledRecordsCount);

    for (int i = 0; i < filledRecordsCount; i++) {
      if (mersenneTwister.nextBoolean()) {
        localPage.deleteRecord(i);
        positions.remove(i);
      }
    }

    int recordsIterated = 0;
    int recordPosition = Integer.MAX_VALUE;
    int lastRecordPosition = Integer.MAX_VALUE;
    do {
      recordPosition = localPage.findLastRecord(recordPosition);
      if (recordPosition < 0)
        break;

      Assert.assertTrue(positions.contains(recordPosition));
      Assert.assertTrue(recordPosition < lastRecordPosition);

      recordPosition--;
      recordsIterated++;
    } while (recordPosition >= 0);

    Assert.assertEquals(recordsIterated, positions.size());
  }

  public void testSetGetNextPage() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      setGetNextPage(localPage);
      setGetNextPage(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void setGetNextPage(OClusterPage localPage) throws IOException {
    localPage.setNextPage(1034);
    Assert.assertEquals(localPage.getNextPage(), 1034);
  }

  public void testSetGetPrevPage() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      setGetPrevPage(localPage);
      setGetPrevPage(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void setGetPrevPage(OClusterPage localPage) throws IOException {
    localPage.setPrevPage(1034);
    Assert.assertEquals(localPage.getPrevPage(), 1034);
  }

  public void testReplaceOneRecordWithBiggerSize() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      replaceOneRecordWithBiggerSize(localPage);
      replaceOneRecordWithBiggerSize(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithBiggerSize(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  public void testReplaceOneRecordWithEqualSize() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      replaceOneRecordWithEqualSize(localPage);
      replaceOneRecordWithEqualSize(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithEqualSize(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  public void testReplaceOneRecordWithSmallerSize() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);

    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      replaceOneRecordWithSmallerSize(localPage);
      replaceOneRecordWithSmallerSize(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordWithSmallerSize(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 6);

    Assert.assertEquals(localPage.getRecordSize(index), 6);

    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 6), new byte[] { 5, 2, 3, 4, 5, 11 });

    Assert.assertEquals(localPage.getRecordVersion(index), newRecordVersion);
  }

  public void testReplaceOneRecordNoVersionUpdate() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      replaceOneRecordNoVersionUpdate(localPage);
      replaceOneRecordNoVersionUpdate(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordNoVersionUpdate(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;
    newRecordVersion++;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, -1);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);

    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  public void testReplaceOneRecordLowerVersion() throws Exception {
    OPageCacheByteBuffersPool byteBuffersPool = new OPageCacheByteBuffersPool(OClusterPage.PAGE_SIZE);
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);

    OCachePointer directCachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0),
        0, 0);
    directCachePointer.incrementReferrer();

    OCacheEntry directCacheEntry = new OCacheEntry(0, 0, directCachePointer, false);
    try {
      OClusterPage localPage = new OClusterPage(cacheEntry, true, new OWALChangesTree());
      OClusterPage directLocalPage = new OClusterPage(directCacheEntry, true, null);

      replaceOneRecordLowerVersion(localPage);
      replaceOneRecordLowerVersion(directLocalPage);

      assertChangesTracking(localPage, directCachePointer.getByteBuffer(), byteBuffersPool);
    } finally {
      cachePointer.decrementReferrer();
      directCachePointer.decrementReferrer();
    }
  }

  private void replaceOneRecordLowerVersion(OClusterPage localPage) throws IOException {
    Assert.assertEquals(localPage.getRecordsCount(), 0);

    int recordVersion = 0;
    recordVersion++;

    int index = localPage.appendRecord(recordVersion, new byte[] { 1, 2, 3, 4, 5, 6, 5, 4, 3, 2, 1 });
    int freeSpace = localPage.getFreeSpace();

    int newRecordVersion = 0;
    newRecordVersion = recordVersion;

    int written = localPage.replaceRecord(index, new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1, 3 }, newRecordVersion);
    Assert.assertEquals(localPage.getFreeSpace(), freeSpace);
    Assert.assertEquals(written, 11);

    Assert.assertEquals(localPage.getRecordSize(index), 11);
    Assert.assertEquals(localPage.getRecordBinaryValue(index, 0, 11), new byte[] { 5, 2, 3, 4, 5, 11, 5, 4, 3, 2, 1 });

    Assert.assertEquals(localPage.getRecordVersion(index), recordVersion);
  }

  private void assertChangesTracking(OClusterPage localPage, ByteBuffer byteBuffer, OPageCacheByteBuffersPool byteBuffersPool)
      throws IOException {
    OCachePointer cachePointer = new OCachePointer(byteBuffersPool, byteBuffersPool.acquire(), new OLogSequenceNumber(0, 0), 0, 0);
    cachePointer.incrementReferrer();

    OCacheEntry cacheEntry = new OCacheEntry(0, 0, cachePointer, false);
    try {
      OClusterPage restoredPage = new OClusterPage(cacheEntry, false, null);

      OWALChangesTree changesTree = localPage.getChangesTree();
      restoredPage.restoreChanges(changesTree);

      byte[] directArray = new byte[OClusterPage.PAGE_SIZE - SYSTEM_OFFSET];
      byteBuffer.position(SYSTEM_OFFSET);
      byteBuffer.get(directArray);

      ByteBuffer restoredByteBuffer = cachePointer.getByteBuffer();
      byte[] restoredArray = new byte[OClusterPage.PAGE_SIZE - SYSTEM_OFFSET];
      restoredByteBuffer.position(SYSTEM_OFFSET);
      restoredByteBuffer.get(restoredArray);

      Assert.assertEquals(restoredArray, directArray);
    } finally {
      cachePointer.decrementReferrer();
    }
  }
}
