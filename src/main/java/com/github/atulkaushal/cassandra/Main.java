package com.github.atulkaushal.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/** The Class Main. */
public class Main {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {

    // If you don’t specify any contact point, the driver defaults to 127.0.0.1:9042
    CqlSession session = CqlSession.builder().build();

    ResultSet rs = session.execute("select release_version from system.local");
    Row row = rs.one();
    System.out.println(row.getString("release_version"));
    session.close();
  }
}
