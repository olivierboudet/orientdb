package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
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
import com.orientechnologies.orient.core.exception.OSinglePageClusterException;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;
import com.orientechnologies.orient.core.storage.*;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.IOException;

import static com.orientechnologies.orient.core.config.OGlobalConfiguration.DISK_CACHE_PAGE_SIZE;
import static com.orientechnologies.orient.core.config.OGlobalConfiguration.PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY;

public class OSinglePageCluster extends ODurableComponent implements OCluster {
  public static final String DEF_EXTENSION = ".spc";

  private static final int DISK_PAGE_SIZE           = DISK_CACHE_PAGE_SIZE.getValueAsInteger();
  private static final int LOWEST_FREELIST_BOUNDARY = PAGINATED_STORAGE_LOWEST_FREELIST_BOUNDARY.getValueAsInteger();
  private final static int FREE_LIST_SIZE           = DISK_PAGE_SIZE - LOWEST_FREELIST_BOUNDARY;

  private static final int PAGE_INDEX_OFFSET    = 16;
  private static final int RECORD_POSITION_MASK = 0xFFFF;

  private static final int ONE_KB                     = 1024;
  public static final int  CLUSTER_ENTRY_TYPE         = 1;
  public static final int  PART_OF_CLUSTER_ENTRY_TYPE = 3;
  public static final int  CLUSTER_ENTRY_LIST_TYPE    = 2;

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
      throw OException
          .wrapException(new OSinglePageClusterException("Error during creation of cluster with name " + getName(), this), e);
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
    final int maxContentSize = OSPCPage.MAX_RECORD_SIZE - 1;
    OAtomicOperation atomicOperation = startAtomicOperation();
    acquireExclusiveLock();
    try {
      final BytesContainer binaryEntry = serializeClusterEntry(content, recordVersion, recordType);
      binaryEntry.offset = 0;

      final int parts = (binaryEntry.bytes.length + maxContentSize - 1) / maxContentSize;

      if (parts == 1) {
        final long recordPosition = addSingleRecord((byte) CLUSTER_ENTRY_TYPE, binaryEntry, binaryEntry.bytes.length, recordVersion,
            atomicOperation);
        endAtomicOperation(false, null);
        return recordPosition;
      }

      long[] positions = new long[4];
      int positionsSize = 0;

      for (int part = 0; part < parts; part++) {
        final int from = part * maxContentSize;
        final int to = Math.min(from + maxContentSize, binaryEntry.bytes.length);

        if (positionsSize + 2 > positions.length) {
          long[] newPositions = new long[positions.length << 1];
          System.arraycopy(positions, 0, newPositions, 0, positions.length);
          positions = newPositions;
        }

        binaryEntry.offset = from;
        final long[] recordPositions = addSingleClusterEntryIndexPosition((byte) PART_OF_CLUSTER_ENTRY_TYPE, binaryEntry, to - from,
            recordVersion, atomicOperation);

        for (int i = 0; i < 2; i++) {
          positions[positionsSize++] = recordPositions[i];
        }

      }
      final BytesContainer binaryEntryList = serializeEntryList(positions, recordVersion);
      binaryEntryList.offset = 0;

      final long recordPosition = addSingleRecord((byte) CLUSTER_ENTRY_LIST_TYPE, binaryEntryList, binaryEntryList.bytes.length,
          recordVersion, atomicOperation);
      endAtomicOperation(false, null);

      return recordPosition;
    } catch (RuntimeException e) {
      endAtomicOperation(true, e);

      throw OException.wrapException(new OSinglePageClusterException("Error during record creation", this), e);
    } finally {
      releaseExclusiveLock();
    }
  }

  private BytesContainer serializeEntryList(long[] positions, int recordVersion) {

    int contentSize = OVarIntSerializer.computeVarInt32Size(positions.length);

    for (int i = 0; i < positions.length;) {
      contentSize += OVarIntSerializer.computeVarInt64Size(positions[i++]);
      contentSize += OVarIntSerializer.computeVarInt32Size((int) positions[i++]);
    }

    contentSize += OVarIntSerializer.computeVarInt32Size(recordVersion);

    final byte[] content = new byte[contentSize];
    final BytesContainer bytesContainer = new BytesContainer(content);
    OVarIntSerializer.writeVarInt32(recordVersion, bytesContainer);

    OVarIntSerializer.writeVarInt32(positions.length, bytesContainer);
    for (int i = 0; i < positions.length;) {
      OVarIntSerializer.writeVarInt64(positions[i++], bytesContainer);
      OVarIntSerializer.writeVarInt32((int) positions[i++], bytesContainer);
    }

    return bytesContainer;
  }

  private long[] deserializeEntryList(BytesContainer bytesContainer) {
    OVarIntSerializer.skipRawVarInt(bytesContainer);

    final int positionsSize = OVarIntSerializer.readVarInt32(bytesContainer);

    final long[] positions = new long[positionsSize];

    for (int i = 0; i < positionsSize;) {
      positions[i++] = OVarIntSerializer.readVarInt64(bytesContainer);
      positions[i++] = OVarIntSerializer.readVarInt32(bytesContainer);
    }

    return positions;
  }

  private BytesContainer serializeClusterEntry(byte[] content, int recordVersion, byte recordType) {
    final int contentLength = OVarIntSerializer.computeVarInt32Size(recordVersion) + content.length
        + OVarIntSerializer.computeVarInt32Size(content.length) + OByteSerializer.BYTE_SIZE;

    final byte[] result = new byte[contentLength];
    final BytesContainer bytesContainer = new BytesContainer(result);

    OVarIntSerializer.writeVarInt32(recordVersion, bytesContainer);

    OVarIntSerializer.writeVarInt32(content.length, bytesContainer);
    System.arraycopy(content, 0, bytesContainer.bytes, bytesContainer.offset, content.length);
    bytesContainer.offset += content.length;

    bytesContainer.bytes[bytesContainer.offset++] = recordType;

    return bytesContainer;
  }

  private ORawBuffer deserializeClusterEntry(BytesContainer bytesContainer) {
    final int recordVersion = OVarIntSerializer.readVarInt32(bytesContainer);

    final int contentLength = OVarIntSerializer.readVarInt32(bytesContainer);
    final byte[] content = new byte[contentLength];

    System.arraycopy(bytesContainer.bytes, bytesContainer.offset, content, 0, contentLength);
    bytesContainer.offset += contentLength;

    final byte recordType = bytesContainer.bytes[bytesContainer.offset++];

    return new ORawBuffer(content, recordVersion, recordType);
  }

  private long[] addSingleClusterEntryIndexPosition(byte recordType, BytesContainer data, int size, int recordVersion,
      OAtomicOperation atomicOperation) throws IOException {
    final FindFreePageResult findFreePageResult = findFreePage(size, atomicOperation);

    final int freePageIndex = findFreePageResult.freePageIndex;
    final long pageIndex = findFreePageResult.pageIndex;

    boolean newRecord = freePageIndex >= FREE_LIST_SIZE;

    final OCacheEntry cacheEntry;
    if (newRecord) {
      cacheEntry = addPage(atomicOperation, fileId);
    } else {
      cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false);
    }

    final int position;
    cacheEntry.acquireExclusiveLock();
    try {
      final OSPCPage localPage = new OSPCPage(cacheEntry, newRecord, getChangesTree(atomicOperation, cacheEntry));
      assert newRecord || freePageIndex == calculateFreePageIndex(localPage);
      position = localPage.appendRecord(recordVersion, recordType, data, size);

      assert position >= 0;
    } finally {
      cacheEntry.releaseExclusiveLock();
      releasePage(atomicOperation, cacheEntry);
    }

    updateFreePagesIndex(freePageIndex, pageIndex, atomicOperation);

    return new long[] { pageIndex, position };
  }

  private long addSingleRecord(byte recordType, BytesContainer data, int size, int recordVersion, OAtomicOperation atomicOperation)
      throws IOException {
    final long[] recordPositions = addSingleClusterEntryIndexPosition(recordType, data, size, recordVersion, atomicOperation);
    return recordPositions[0] << 16 | recordPositions[1];
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
          final OSPCPage localPage = new OSPCPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
          final int firstRecord = localPage.findFirstRecord(0, new byte[] {CLUSTER_ENTRY_TYPE, CLUSTER_ENTRY_LIST_TYPE});

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

      long[] entryList = null;

      int recordPosition = -1;
      long recordPageIndex = -1;

      for (; pageIndex < filledUpTo; pageIndex++) {
        final OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 0);
        cacheEntry.acquireSharedLock();
        try {
          final OSPCPage localPage = new OSPCPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));

          recordPosition = localPage.findFirstRecord(pagePosition, new byte[] { CLUSTER_ENTRY_TYPE, CLUSTER_ENTRY_LIST_TYPE });
          if (recordPosition >= 0) {
            recordPageIndex = pageIndex;

            final BytesContainer binaryClusterEntry = localPage.getRecordAndType(recordPosition);
            binaryClusterEntry.offset = 1;

            if (binaryClusterEntry.bytes[0] == CLUSTER_ENTRY_TYPE) {
              return new RawRecord(deserializeClusterEntry(binaryClusterEntry), recordPageIndex << 16 | recordPosition);
            } else
              entryList = deserializeEntryList(binaryClusterEntry);

            break;
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
    public final ORawBuffer data;
    public final long       position;

    public RawRecord(ORawBuffer data, long position) {
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
            OSPCPage localPage = new OSPCPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
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

  private int calculateFreePageIndex(OSPCPage localPage) {
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
      final OSPCPage localPage = new OSPCPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
      int newFreePageIndex = calculateFreePageIndex(localPage);

      if (prevFreePageIndex == newFreePageIndex)
        return;

      long nextPageIndex = localPage.getNextPage();
      long prevPageIndex = localPage.getPrevPage();

      if (prevPageIndex >= 0) {
        final OCacheEntry prevPageCacheEntry = loadPage(atomicOperation, fileId, prevPageIndex, false);
        prevPageCacheEntry.acquireExclusiveLock();
        try {
          final OSPCPage prevPage = new OSPCPage(prevPageCacheEntry, false, getChangesTree(atomicOperation, prevPageCacheEntry));
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
          final OSPCPage nextPage = new OSPCPage(nextPageCacheEntry, false, getChangesTree(atomicOperation, nextPageCacheEntry));
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
            final OSPCPage oldFreeLocalPage = new OSPCPage(oldFreePageCacheEntry, false,
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

  private ORawBuffer doReadRecord(OAtomicOperation atomicOperation, long pageIndex, int pagePosition) throws IOException {
    long[] entryList = null;

    OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 0);
    cacheEntry.acquireSharedLock();
    try {
      final OSPCPage localPage = new OSPCPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
      BytesContainer binaryClusterEntry = localPage.getRecordAndType(pagePosition);
      binaryClusterEntry.offset = 1;
      switch (binaryClusterEntry.bytes[0]) {

      case CLUSTER_ENTRY_TYPE: {
        return deserializeClusterEntry(binaryClusterEntry);
      }

      case CLUSTER_ENTRY_LIST_TYPE: {
        entryList = deserializeEntryList(binaryClusterEntry);
        break;
      }

      default: {
        throw new OSinglePageClusterException("Invalid record type " + binaryClusterEntry.bytes[0], this);
      }
      }

    } finally {
      cacheEntry.releaseSharedLock();
      releasePage(atomicOperation, cacheEntry);
    }

    return readRecordFromEntryList(atomicOperation, entryList);
  }

  private ORawBuffer readRecordFromEntryList(OAtomicOperation atomicOperation, long[] entryList) throws IOException {
    BytesContainer bytesContainer = new BytesContainer(new byte[OSPCPage.MAX_RECORD_SIZE << 1]);
    for (int i = 0; i < entryList.length;) {
      final long pageIndex = entryList[i++];
      final int recordPosition = (int) entryList[i++];

      OCacheEntry cacheEntry = loadPage(atomicOperation, fileId, pageIndex, false, 0);
      cacheEntry.acquireSharedLock();
      try {
        final OSPCPage localPage = new OSPCPage(cacheEntry, false, getChangesTree(atomicOperation, cacheEntry));
        byte[] binaryClusterEntry = localPage.getRecordBinaryValue(recordPosition, 1);

        final int offset = bytesContainer.alloc(binaryClusterEntry.length);
        System.arraycopy(binaryClusterEntry, 0, bytesContainer.bytes, offset, binaryClusterEntry.length);
      } finally {
        cacheEntry.releaseSharedLock();
        releasePage(atomicOperation, cacheEntry);
      }
    }

    bytesContainer.offset = 0;
    return deserializeClusterEntry(bytesContainer);
  }

  @Override
  public ORawBuffer readRecord(long clusterPosition) throws IOException {
    acquireSharedLock();
    try {
      final OAtomicOperation atomicOperation = atomicOperationsManager.getCurrentOperation();
      final long filledUpTo = getFilledUpTo(atomicOperation, fileId);

      long pageIndex = (clusterPosition >>> 16);
      int pagePosition = (int) (clusterPosition & 0xFFFFL);

      if (pageIndex >= filledUpTo)
        return null;

      return doReadRecord(atomicOperation, pageIndex, pagePosition);
    } finally {
      releaseSharedLock();
    }
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
