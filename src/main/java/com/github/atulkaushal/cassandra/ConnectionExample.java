package com.github.atulkaushal.cassandra;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;

/**
 * The Class ConnectionExample.
 *
 * @author Atul
 */
public class ConnectionExample {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {

    CqlSession session =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress("localhost", 9042))
            .withLocalDatacenter("datacenter1")
            .withKeyspace("hotel")
            // .withAuthCredentials(null, null)
            .build();

    Metadata metaData = session.getMetadata();
    System.out.println(metaData.getClusterName().get());

    Map<CqlIdentifier, KeyspaceMetadata> keyspaceMap = metaData.getKeyspaces();
    Iterator<CqlIdentifier> keyspaceMapIterator = keyspaceMap.keySet().iterator();
    while (keyspaceMapIterator.hasNext()) {
      KeyspaceMetadata keySpaceMetaData = keyspaceMap.get(keyspaceMapIterator.next());
      System.out.println(
          "KeySpace Name: "
              + keySpaceMetaData.getName()
              + "  Replication Factor:"
              + keySpaceMetaData.getReplication());
    }
    session.close();
  }
}
