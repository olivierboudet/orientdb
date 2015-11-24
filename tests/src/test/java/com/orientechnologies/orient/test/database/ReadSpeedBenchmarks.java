package com.orientechnologies.orient.test.database;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.testng.annotations.Test;

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
}