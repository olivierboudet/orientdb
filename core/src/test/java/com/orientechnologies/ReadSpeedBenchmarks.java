package com.orientechnologies;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OSinglePageCluster;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test
public class ReadSpeedBenchmarks {
  public void fullQueryBenchmark() {
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("plocal:E:\\pokec");
    databaseDocumentTx.open("admin", "admin");

    for (int i = 0; i < 100; i++) {
      databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select AGE, count(*) from Profile group by AGE limit -1"));
    }

    long start = System.nanoTime();
    for (int i = 0; i < 100; i++) {
      databaseDocumentTx.query(new OSQLSynchQuery<ODocument>("select AGE, count(*) from Profile group by AGE limit -1"));
    }
    long end = System.nanoTime();

    System.out.println((end - start) / (100L * 1000000L));
  }

  public void diskReadBenchmark() throws Exception {
    OFileClassic fileClassic = new OFileClassic("E:\\pokec\\profile_1.pcl", "rw");
    fileClassic.open();

    final int pageSize = 65536;
    final long pages = fileClassic.getFileSize() / pageSize;

    for (int n = 0; n < 100; n++) {
      for (int i = 0; i < pages; i++) {
        final byte[] page = new byte[pageSize];
        fileClassic.read(i * pageSize, page, pageSize, 0);
      }
    }

    long start = System.nanoTime();
    for (int n = 0; n < 100; n++) {
      for (int i = 0; i < pages; i++) {
        final byte[] page = new byte[pageSize];
        fileClassic.read(i * pageSize, page, pageSize, 0);
      }
    }
    long end = System.nanoTime();

    System.out.println((15L * (end - start) / (100 * 1000000L)));
  }

  public void clusterReadBenchmark() throws Exception {
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("plocal:E:\\pokec");
    databaseDocumentTx.open("admin", "admin");

    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();

    OCluster cluster = paginatedStorage.getClusterByName("profile");
    // for (int i = 0; i < 100; i++) {
    // OPhysicalPosition[] positionsToProcess = cluster.ceilingPositions(new OPhysicalPosition(-1));
    // while (positionsToProcess.length > 0) {
    // for (OPhysicalPosition positionsToProces : positionsToProcess) {
    // cluster.readRecord(positionsToProces.clusterPosition);
    // }
    // positionsToProcess = cluster.higherPositions(positionsToProcess[positionsToProcess.length - 1]);
    // }
    // }

    long start = System.nanoTime();
    // for (int n = 0; n < 100; n++) {
    OPhysicalPosition[] positionsToProcess = cluster.ceilingPositions(new OPhysicalPosition(-1));
    while (positionsToProcess.length > 0) {
      for (OPhysicalPosition positionsToProces : positionsToProcess) {
        cluster.readRecord(positionsToProces.clusterPosition);
      }
      positionsToProcess = cluster.higherPositions(positionsToProcess[positionsToProcess.length - 1]);
    }
    // }

    long end = System.nanoTime();

    System.out.println((15L * (end - start) / (1000000L)));

    databaseDocumentTx.close();
  }

  public void createSinglePageCluster() throws Exception {
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("plocal:E:\\pokec");
    databaseDocumentTx.open("admin", "admin");

    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();

    OCluster cluster = paginatedStorage.getClusterByName("profile");

    OSinglePageCluster singlePageCluster = new OSinglePageCluster(paginatedStorage, "profile");
    singlePageCluster.create(0);

    long records = 0;
    OPhysicalPosition[] positionsToProcess = cluster.ceilingPositions(new OPhysicalPosition(-1));

    while (positionsToProcess.length > 0) {
      for (OPhysicalPosition positionsToProces : positionsToProcess) {
        ORawBuffer buffer = cluster.readRecord(positionsToProces.clusterPosition);
        long pos = singlePageCluster.createNewRecord(buffer.buffer, buffer.version, buffer.recordType);
        ORawBuffer rec = singlePageCluster.readRecord(pos);
        assert Arrays.equals(rec.buffer, buffer.buffer);
        records++;
      }

      positionsToProcess = cluster.higherPositions(positionsToProcess[positionsToProcess.length - 1]);
    }

    singlePageCluster.close();
    databaseDocumentTx.close();

    System.out.println(records);
  }

  public void singlePageClusterReadBenchmark() throws Exception {
    ODatabaseDocumentTx databaseDocumentTx = new ODatabaseDocumentTx("plocal:E:\\pokec");
    databaseDocumentTx.open("admin", "admin");

    final OAbstractPaginatedStorage paginatedStorage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();

    OSinglePageCluster singlePageCluster = new OSinglePageCluster(paginatedStorage, "profile");
    singlePageCluster.open();

    for (int n = 0; n < 100; n++) {
      long recordPosition = singlePageCluster.firstRecordPosition();
      if (recordPosition < 0)
        break;

      ORawBuffer data = singlePageCluster.readRecord(recordPosition);

      OSinglePageCluster.RawRecord rawRecord = null;
      do {
        rawRecord = singlePageCluster.readNexRecord(recordPosition);
        if (rawRecord != null) {
          recordPosition = rawRecord.position;
        }

      } while (rawRecord != null);
    }

    long start = System.nanoTime();
    for (int n = 0; n < 100; n++) {
      long recordPosition = singlePageCluster.firstRecordPosition();

      ORawBuffer data = singlePageCluster.readRecord(recordPosition);
      OSinglePageCluster.RawRecord rawRecord;
      do {
        rawRecord = singlePageCluster.readNexRecord(recordPosition);
        if (rawRecord != null) {
          recordPosition = rawRecord.position;
        }

      } while (rawRecord != null);

    }
    long end = System.nanoTime();

    System.out.println((15L * (end - start) / (100 * 1000000L)));
    System.out.println("staff ----");
  }
}
