package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.google.protobuf.ByteString;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.protobuf.DocumentProtobufSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.compression.OCompression;
import com.orientechnologies.orient.core.compression.OCompressionFactory;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePaginatedClusterConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;

public class OSinglePageCluster extends ODurableComponent implements OCluster {
  public static final String DEF_EXTENSION = ".spc";

  private static final int DISK_PAGE_SIZE           = DISK_CACHE_PAGE_SIZE.getValueAsInteger();
  private static final int LOWEST_FREELIST_BOUNDARY = PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
  private final static int FREE_LIST_SIZE           = DISK_PAGE_SIZE - LOWEST_FREELIST_BOUNDARY;

  private static final int PAGE_INDEX_OFFSET    = 16;
  private static final int RECORD_POSITION_MASK = 0xFFFF;
  private static final int ONE_KB               = 1024;

  private volatile OCompression                 compression;
  private volatile OEncryption                  encryption;
  private volatile String                       encryptionKey;
  private OAbstractPaginatedStorage             storageLocal;
  private volatile int                          id;
  private long                                  fileId;
  private OStoragePaginatedClusterConfiguration config;
  private long                                  pinnedStateEntryIndex;
  private ORecordConflictStrategy               recordConflictStrategy;

  public OSinglePageCluster(OAbstractPaginatedStorage storage, String name) {
    super(storage, name, DEF_EXTENSION);
  }

  @Override
  public void configure(OStorage iStorage, int iId, String clusterName, Object... iParameters) throws IOException {
    acquireExclusiveLock();
    try {
      final OContextConfiguration ctxCfg = storage.getConfiguration().getContextConfiguration();
      final String cfgCompression = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_COMPRESSION_METHOD);
      final String cfgEncryption = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_METHOD);
      final String cfgEncryptionKey = ctxCfg.getValueAsString(OGlobalConfiguration.STORAGE_ENCRYPTION_KEY);

      config = new OStoragePaginatedClusterConfiguration(storage.getConfiguration(), id, clusterName, null, true,
          OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR, OStoragePaginatedClusterConfiguration.DEFAULT_GROW_FACTOR,
          cfgCompression, cfgEncryption, cfgEncryptionKey, null, OStorageClusterConfiguration.STATUS.ONLINE);
      config.name = clusterName;

      init(storage, config);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void configure(OStorage iStorage, OStorageClusterConfiguration iConfig) throws IOException {
    acquireExclusiveLock();
    try {
      init(storage, config);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void init(final OAbstractPaginatedStorage storage, final OStorageClusterConfiguration config) throws IOException {
    OFileUtils.checkValidName(config.getName());

    this.config = (OStoragePaginatedClusterConfiguration) config;
    this.compression = OCompressionFactory.INSTANCE.getCompression(this.config.compression, null);
    this.encryption = OEncryptionFactory.INSTANCE.getEncryption(this.config.encryption, this.config.encryptionKey);

    if (((OStoragePaginatedClusterConfiguration) config).conflictStrategy != null)
      this.recordConflictStrategy = Orient.instance().getRecordConflictStrategy()
          .getStrategy(((OStoragePaginatedClusterConfiguration) config).conflictStrategy);

    storageLocal = storage;

    this.id = config.getId();
  }

  @Override
  public void create(int iStartSize) throws IOException {
    final OAtomicOperation atomicOperation = startAtomicOperation();
    acquireExclusiveLock();
    try {
      fileId = addFile(atomicOperation, getFullName());

      initCusterState(atomicOperation);

      // if (config.root.clusters.size() <= config.id)
      // config.root.clusters.add(config);
      // else
      // config.root.clusters.set(config.id, config);

      endAtomicOperation(false, null);
    } catch (Exception e) {
      endAtomicOperation(true, e);
      throw new RuntimeException(e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private void initCusterState(OAtomicOperation atomicOperation) throws IOException {
    OCacheEntry pinnedStateEntry = addPage(atomicOperation, fileId);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry,
          getChangesTree(atomicOperation, pinnedStateEntry));

      pinPage(atomicOperation, pinnedStateEntry);
      paginatedClusterState.setSize(0);
      paginatedClusterState.setRecordsSize(0);

      for (int i = 0; i < FREE_LIST_SIZE; i++)
        paginatedClusterState.setFreeListPage(i, -1);

      pinnedStateEntryIndex = pinnedStateEntry.getPageIndex();
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      releasePage(atomicOperation, pinnedStateEntry);
    }
  }

  @Override
  public void open() throws IOException {
    acquireExclusiveLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      fileId = openFile(atomicOperation, getFullName());

      final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, 0, false);
      try {
        pinPage(atomicOperation, pinnedStateEntry);
        pinnedStateEntryIndex = pinnedStateEntry.getPageIndex();
      } finally {
        releasePage(atomicOperation, pinnedStateEntry);
      }
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void close() throws IOException {
    close(true);
  }

  public void close(final boolean flush) throws IOException {
    acquireExclusiveLock();
    try {
      if (flush)
        synch();

      readCache.closeFile(fileId, flush, writeCache);
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public void delete() throws IOException {
  }

  @Override
  public Object set(ATTRIBUTES iAttribute, Object iValue) throws IOException {
    return null;
  }

  @Override
  public String encryption() {
    return null;
  }

  @Override
  public long getTombstonesCount() {
    return 0;
  }

  @Override
  public void truncate() throws IOException {
  }

  public long createNewRecord(byte[] content, int recordVersion, byte recordType) throws IOException {
    final int maxContentSize = OClusterPage.MAX_RECORD_SIZE - 1;
    OAtomicOperation atomicOperation = startAtomicOperation();
    acquireExclusiveLock();
    try {
      DocumentProtobufSerializer.ClusterEntry.Builder builder = DocumentProtobufSerializer.ClusterEntry.newBuilder();
      builder.setType(recordType);
      builder.setVersion(recordVersion);
      builder.setRaw(ByteString.copyFrom(content));

      final ByteArrayOutputStream stream = new ByteArrayOutputStream();
      DocumentProtobufSerializer.ClusterEntry entry = builder.build();
      entry.writeTo(stream);

      content = stream.toByteArray();

      final int parts = (content.length + maxContentSize - 1) / maxContentSize;

      if (parts == 1) {
        final long recordPosition = addSingleClusterPosition((byte) 1, content, recordVersion, atomicOperation);
        endAtomicOperation(false, null);
        return recordPosition;
      }

      final List<Long> positions = new ArrayList<Long>();
      for (int part = 0; part < parts; part++) {
        final int from = part * maxContentSize;
        final int to = Math.min(from + maxContentSize, content.length);
        positions.add(addSingleClusterPosition((byte) 0, Arrays.copyOfRange(content, from, to), recordVersion, atomicOperation));
      }

      final ByteArrayOutputStream entryStream = new ByteArrayOutputStream();
      DocumentProtobufSerializer.ClusterEntry.Builder entryListBuilder = DocumentProtobufSerializer.ClusterEntry.newBuilder();
      DocumentProtobufSerializer.EntryList.Builder listBuilder = DocumentProtobufSerializer.EntryList.newBuilder();
      listBuilder.addAllPositions(positions);

      entryListBuilder.setPositions(listBuilder);
      entryListBuilder.setType(recordType);
      entryListBuilder.setVersion(recordVersion);

      DocumentProtobufSerializer.ClusterEntry entryList = entryListBuilder.build();
      entryList.writeTo(entryStream);

      final long recordPosition = addSingleClusterPosition((byte) 1, entryStream.toByteArray(), recordVersion, atomicOperation);
      endAtomicOperation(false, null);

      return recordPosition;
    } catch (RuntimeException e) {
      endAtomicOperation(true, e);
      throw e;
    } finally {
      releaseExclusiveLock();
    }
  }

  private long addSingleClusterPosition(byte recordType, byte[] data, int recordVersion, OAtomicOperation atomicOperation)
      throws IOException {
    final FindFreePageResult findFreePageResult = findFreePage(data.length, atomicOperation);

    final int freePageIndex = findFreePageResult.freePageIndex;
    final long pageIndex = findFreePageResult.pageIndex;

    boolean newRecord = freePageIndex >= FREE_LIST_SIZE;

    OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
    if (cacheEntry == null)
      cacheEntry = addPage(atomicOperation, fileId);

    final int position;

    cacheEntry.acquireExclusiveLock();
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, newRecord, getChangesTree(atomicOperation, cacheEntry));
      assert newRecord || freePageIndex == calculateFreePageIndex(localPage);

      position = localPage.appendRecord(recordVersion, data, 1, 0, data.length + 1);
      localPage.setRecordByteValue(position, 0, recordType);

      assert position >= 0;
    } finally {
      cacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, cacheEntry);
    }

    updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);

    return pageIndex << 16 | position;
  }

  public long firstRecordPosition() throws IOException {
    acquireSharedLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      for (long pageIndex = 1; pageIndex < filledUpTo; pageIndex++) {
        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 0);
        cacheEntry.acquireSharedLock();
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
          final int firstRecord = localPage.findFirstRecord(0);
          if (firstRecord > -1)
            return pageIndex << 16 | firstRecord;
        } finally {
          cacheEntry.releaseSharedLock();
          releasePage(atomicOperation, cacheEntry);
        }
      }
    } finally {
      releaseSharedLock();
    }

    return -1;
  }

  public RawRecord readNexRecord(long position) throws IOException {
    acquireSharedLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      long pageIndex = (position >>> 16);
      int pagePosition = (int) (position & 0xFFFFL) + 1;

      DocumentProtobufSerializer.EntryList entryList = null;

      int recordPosition = -1;
      long recordPageIndex = -1;

      recordLoop: for (; pageIndex < filledUpTo; pageIndex++) {
        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 0);
        cacheEntry.acquireSharedLock();
        try {
          final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));

          recordPosition = localPage.findFirstRecord(pagePosition);
          while (recordPosition > -1) {
            if (localPage.getRecordByteValue(recordPosition, 0) > 0) {
              recordPageIndex = pageIndex;

              byte[] binaryClusterEntry = localPage.getRecordBinaryValue(recordPosition, 1);
              DocumentProtobufSerializer.ClusterEntry entry = DocumentProtobufSerializer.ClusterEntry.parseFrom(binaryClusterEntry);

              if (entry.getDataCase().equals(DocumentProtobufSerializer.ClusterEntry.DataCase.RAW)) {
                return new RawRecord(entry.getRaw().toByteArray(), recordPageIndex << 16 | recordPosition);
              } else
                entryList = entry.getPositions();

              break recordLoop;
            } else {
              pagePosition++;
              recordPosition = localPage.findFirstRecord(pagePosition);
            }
          }

          pagePosition = 0;
        } finally {
          cacheEntry.releaseSharedLock();
          releasePage(atomicOperation, cacheEntry);
        }
      }

      if (entryList == null)
        return null;

      return new RawRecord(readRecordFromEntryList(atomicOperation, entryList), recordPageIndex << 16 | recordPosition);
    } finally {
      releaseSharedLock();
    }
  }

  public static class RawRecord {
    public final byte[] data;
    public final long   position;

    public RawRecord(byte[] data, long position) {
      this.data = data;
      this.position = position;
    }
  }

  private FindFreePageResult findFreePage(int contentSize, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
    try {
      while (true) {
        int freePageIndex = contentSize / ONE_KB;
        freePageIndex -= PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
        if (freePageIndex < 0)
          freePageIndex = 0;

        OPaginatedClusterState freePageLists = new OPaginatedClusterState(pinnedStateEntry,
            getChangesTree(atomicOperation, pinnedStateEntry));
        long pageIndex;
        do {
          pageIndex = freePageLists.getFreeListPage(freePageIndex);
          freePageIndex++;
        } while (pageIndex < 0 && freePageIndex < FREE_LIST_SIZE);

        if (pageIndex < 0)
          pageIndex = getFilledUpTo(atomicOperation, fileId);
        else
          freePageIndex--;

        if (freePageIndex < FREE_LIST_SIZE) {
          OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
          int realFreePageIndex;
          try {
            OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
            realFreePageIndex = calculateFreePageIndex(localPage);
          } finally {
            releasePage(atomicOperation, cacheEntry);
          }

          if (realFreePageIndex != freePageIndex) {
            OLogManager.instance().warn(this,
                "Page in file %s with index %d was placed in wrong free list, this error will be fixed automatically.",
                getFullName(), pageIndex);

            updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);
            continue;
          }
        }

        return new FindFreePageResult(pageIndex, freePageIndex);
      }
    } finally {
      releasePage(atomicOperation, pinnedStateEntry);
    }
  }

  private int calculateFreePageIndex(OClusterPage localPage) {
    int newFreePageIndex;
    if (localPage.isEmpty())
      newFreePageIndex = FREE_LIST_SIZE - 1;
    else {
      newFreePageIndex = (localPage.getMaxRecordSize() - (ONE_KB - 1)) / ONE_KB;

      newFreePageIndex -= LOWEST_FREELIST_BOUNDARY;
    }
    return newFreePageIndex;
  }

  private void updateFreePagesIndex(int prevFreePageIndex, long pageIndex, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);

    cacheEntry.acquireExclusiveLock();
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
      int newFreePageIndex = calculateFreePageIndex(localPage);

      if (prevFreePageIndex == newFreePageIndex)
        return;

      long nextPageIndex = localPage.getNextPage();
      long prevPageIndex = localPage.getPrevPage();

      if (prevPageIndex >= 0) {
        final OCacheEntry prevPageCacheEntry = loadPage(atomicOperation, fileId, prevPageIndex, false);
        prevPageCacheEntry.acquireExclusiveLock();
        try {
          final OClusterPage prevPage = new OClusterPage(prevPageCacheEntry, false,
              getChangesTree(atomicOperation, prevPageCacheEntry));
          assert calculateFreePageIndex(prevPage) == prevFreePageIndex;
          prevPage.setNextPage(nextPageIndex);
        } finally {
          prevPageCacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, prevPageCacheEntry);
        }
      }

      if (nextPageIndex >= 0) {
        final OCacheEntry nextPageCacheEntry = loadPage(atomicOperation, fileId, nextPageIndex, false);
        nextPageCacheEntry.acquireExclusiveLock();
        try {
          final OClusterPage nextPage = new OClusterPage(nextPageCacheEntry, false,
              getChangesTree(atomicOperation, nextPageCacheEntry));
          if (calculateFreePageIndex(nextPage) != prevFreePageIndex)
            calculateFreePageIndex(nextPage);

          assert calculateFreePageIndex(nextPage) == prevFreePageIndex;
          nextPage.setPrevPage(prevPageIndex);

        } finally {
          nextPageCacheEntry.releaseExclusiveLock();
          releasePage(atomicOperation, nextPageCacheEntry);
        }
      }

      localPage.setNextPage(-1);
      localPage.setPrevPage(-1);

      if (prevFreePageIndex < 0 && newFreePageIndex < 0)
        return;

      if (prevFreePageIndex >= 0 && prevFreePageIndex < FREE_LIST_SIZE) {
        if (prevPageIndex < 0)
          updateFreePagesList(prevFreePageIndex, nextPageIndex, atomicOperation);
      }

      if (newFreePageIndex >= 0) {
        long oldFreePage;
        OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
        try {
          OPaginatedClusterState clusterFreeList = new OPaginatedClusterState(pinnedStateEntry,
              getChangesTree(atomicOperation, pinnedStateEntry));
          oldFreePage = clusterFreeList.getFreeListPage(newFreePageIndex);
        } finally {
          releasePage(atomicOperation, pinnedStateEntry);
        }

        if (oldFreePage >= 0) {
          final OCacheEntry oldFreePageCacheEntry = loadPage(atomicOperation, fileId, oldFreePage, false);
          oldFreePageCacheEntry.acquireExclusiveLock();
          try {
            final OClusterPage oldFreeLocalPage = new OClusterPage(oldFreePageCacheEntry, false,
                getChangesTree(atomicOperation, oldFreePageCacheEntry));
            assert calculateFreePageIndex(oldFreeLocalPage) == newFreePageIndex;

            oldFreeLocalPage.setPrevPage(pageIndex);
          } finally {
            oldFreePageCacheEntry.releaseExclusiveLock();
            releasePage(atomicOperation, oldFreePageCacheEntry);
          }

          localPage.setNextPage(oldFreePage);
          localPage.setPrevPage(-1);
        }

        updateFreePagesList(newFreePageIndex, pageIndex, atomicOperation);
      }
    } finally {
      cacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, cacheEntry);
    }
  }

  private void updateFreePagesList(int freeListIndex, long pageIndex, OAtomicOperation atomicOperation) throws IOException {
    final OCacheEntry pinnedStateEntry = loadPage(atomicOperation, fileId, pinnedStateEntryIndex, true);
    pinnedStateEntry.acquireExclusiveLock();
    try {
      OPaginatedClusterState paginatedClusterState = new OPaginatedClusterState(pinnedStateEntry,
          getChangesTree(atomicOperation, pinnedStateEntry));
      paginatedClusterState.setFreeListPage(freeListIndex, pageIndex);
    } finally {
      pinnedStateEntry.releaseExclusiveLock();
      releasePage(atomicOperation, pinnedStateEntry);
    }
  }

  private static final class FindFreePageResult {
    private final long pageIndex;
    private final int  freePageIndex;

    private FindFreePageResult(long pageIndex, int freePageIndex) {
      this.pageIndex = pageIndex;
      this.freePageIndex = freePageIndex;
    }
  }

  @Override
  public OPhysicalPosition createRecord(byte[] content, int recordVersion, byte recordType) throws IOException {
    return null;
  }

  @Override
  public boolean deleteRecord(long clusterPosition) throws IOException {
    return false;
  }

  @Override
  public void updateRecord(long clusterPosition, byte[] content, int recordVersion, byte recordType) throws IOException {
  }

  public byte[] readNewRecord(long position) throws Exception {
    acquireSharedLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      long pageIndex = (position >>> 16);
      int pagePosition = (int) (position & 0xFFFFL);

      if (pageIndex >= filledUpTo)
        return null;

      return doReadRecord(atomicOperation, pageIndex, pagePosition);
    } finally {
      releaseSharedLock();
    }
  }

  private byte[] doReadRecord(OAtomicOperation atomicOperation, long pageIndex, int pagePosition) throws IOException {
    DocumentProtobufSerializer.EntryList entryList = null;
    OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 0);
    cacheEntry.acquireSharedLock();
    try {
      final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
      byte[] binaryClusterEntry = localPage.getRecordBinaryValue(pagePosition, 1);
      DocumentProtobufSerializer.ClusterEntry entry = DocumentProtobufSerializer.ClusterEntry.parseFrom(binaryClusterEntry);

      if (entry.getDataCase().equals(DocumentProtobufSerializer.ClusterEntry.DataCase.RAW)) {
        return entry.getRaw().toByteArray();
      } else
        entryList = entry.getPositions();
    } finally {
      cacheEntry.releaseSharedLock();
      releasePage(atomicOperation, cacheEntry);
    }

    return readRecordFromEntryList(atomicOperation, entryList);
  }

  private byte[] readRecordFromEntryList(OAtomicOperation atomicOperation, DocumentProtobufSerializer.EntryList entryList)
      throws IOException {
    long pageIndex;
    int pagePosition;
    OCacheEntry cacheEntry;
    BytesContainer bytesContainer = new BytesContainer(new byte[OClusterPage.MAX_RECORD_SIZE << 1]);
    for (long entryPos : entryList.getPositionsList()) {
      pageIndex = (entryPos >>> 16);
      pagePosition = (int) (entryPos & 0xFFFFL);

      cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 0);
      cacheEntry.acquireSharedLock();
      try {
        final OClusterPage localPage = new OClusterPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
        byte[] binaryClusterEntry = localPage.getRecordBinaryValue(pagePosition, 1);

        final int offset = bytesContainer.alloc(binaryClusterEntry.length);
        System.arraycopy(binaryClusterEntry, 0, bytesContainer.bytes, offset, binaryClusterEntry.length);
      } finally {
        cacheEntry.releaseSharedLock();
        releasePage(atomicOperation, cacheEntry);
      }
    }

    DocumentProtobufSerializer.ClusterEntry entry = DocumentProtobufSerializer.ClusterEntry
        .parseFrom(ByteString.copyFrom(bytesContainer.bytes, 0, bytesContainer.offset));
    return entry.getRaw().toByteArray();
  }

  @Override
  public ORawBuffer readRecord(long clusterPosition) throws IOException {
    return null;
  }

  @Override
  public ORawBuffer readRecordIfVersionIsNotLatest(long clusterPosition, int recordVersion)
      throws IOException, ORecordNotFoundException {
    return null;
  }

  @Override
  public boolean exists() {
    return false;
  }

  @Override
  public OPhysicalPosition getPhysicalPosition(OPhysicalPosition iPPosition) throws IOException {
    return null;
  }

  @Override
  public long getEntries() {
    return 0;
  }

  @Override
  public long getFirstPosition() throws IOException {
    return 0;
  }

  @Override
  public long getLastPosition() throws IOException {
    return 0;
  }

  @Override
  public String getFileName() {
    return null;
  }

  @Override
  public int getId() {
    return 0;
  }

  @Override
  public void synch() throws IOException {

  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public long getRecordsSize() throws IOException {
    return 0;
  }

  @Override
  public float recordGrowFactor() {
    return 0;
  }

  @Override
  public float recordOverflowGrowFactor() {
    return 0;
  }

  @Override
  public String compression() {
    return null;
  }

  @Override
  public boolean isHashBased() {
    return false;
  }

  @Override
  public boolean isSystemCluster() {
    return false;
  }

  @Override
  public OClusterEntryIterator absoluteIterator() {
    return null;
  }

  @Override
  public OPhysicalPosition[] higherPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public OPhysicalPosition[] ceilingPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public OPhysicalPosition[] lowerPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public OPhysicalPosition[] floorPositions(OPhysicalPosition position) throws IOException {
    return new OPhysicalPosition[0];
  }

  @Override
  public boolean hideRecord(long position) throws IOException {
    return false;
  }

  @Override
  public ORecordConflictStrategy getRecordConflictStrategy() {
    return null;
  }
}
