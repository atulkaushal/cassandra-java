package com.github.atulkaushal.cassandra;

import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

/**
 * The Class SchemaBuilderExample.
 *
 * @author Atul
 */
public class SchemaBuilderExample {

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
            .build();

    createTable(session);
    dropTable(session);

    session.close();
    System.exit(0);
  }

  /**
   * Creates the table.
   *
   * @param session the session
   */
  private static void createTable(CqlSession session) {
    SimpleStatement createTableStmt =
        SchemaBuilder.createTable("hotels_temp")
            .ifNotExists()
            .withPartitionKey("state", DataTypes.TEXT)
            .withColumn("id", DataTypes.INT)
            .withClusteringColumn("name", DataTypes.TEXT)
            .withColumn("description", DataTypes.TEXT)
            .withClusteringOrder("name", ClusteringOrder.DESC)
            .build();

    session.execute(createTableStmt);
    System.out.println("Table Created.");
  }

  /**
   * Drop table.
   *
   * @param session the session
   */
  private static void dropTable(CqlSession session) {
    SimpleStatement dropTableStmt = SchemaBuilder.dropTable("hotels_temp").ifExists().build();
    session.execute(dropTableStmt);
    System.out.println("Table Dropped.");
  }
}
