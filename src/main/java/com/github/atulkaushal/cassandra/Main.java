package com.github.atulkaushal.cassandra;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/** The Class Main. 
 * @author Atul
 * */
public class Main {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {

    // If you don’t specify any contact point, the driver defaults to 127.0.0.1:9042
    // CqlSession session = CqlSession.builder().build();

    // CQLsession with default Keyspace
    CqlSession session =
        CqlSession.builder()
            .addContactPoint(new InetSocketAddress("localhost", 9042))
            .withLocalDatacenter("datacenter1")
            .withKeyspace("reservation")
            .build();

    ResultSet rs = session.execute("select release_version from system.local");
    Row row0 = rs.one();
    System.out.println(row0.getString("release_version"));

    session.close();
  }
}
