package com.github.atulkaushal.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

/**
 * The Class PreparedStatementExample.
 *
 * @author Atul
 */
public class PreparedStatementExample {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {

    CqlSession session = CqlSession.builder().withKeyspace("hotel").build();
    insertRow(session);

    ResultSet hotelSelectResult = fetchRow(session);

    printRow(hotelSelectResult);
    session.close();
    System.exit(0);
  }

  /**
   * Prints the row.
   *
   * @param hotelSelectResult the hotel select result
   */
  private static void printRow(ResultSet hotelSelectResult) {
    for (Row row : hotelSelectResult) {
      System.out.format(
          "id: %s, name: %s, phone: %s\n",
          row.getString("id"), row.getString("name"), row.getString("phone") // ,
          );
    }
  }

  /**
   * Fetch row.
   *
   * @param session the session 1
   * @return the result set
   */
  private static ResultSet fetchRow(CqlSession session) {
    PreparedStatement hotelSelectPrepared = session.prepare("SELECT * FROM hotels WHERE id=?");
    BoundStatement hotelSelectBound = hotelSelectPrepared.bind("STL102");
    ResultSet hotelSelectResult = session.execute(hotelSelectBound);
    return hotelSelectResult;
  }

  /**
   * Insert row.
   *
   * @param session the session
   */
  private static void insertRow(CqlSession session) {
    PreparedStatement hotelInsertPrepared =
        session.prepare("INSERT INTO hotel.hotels (id,name,phone) VALUES (?, ?, ?)");

    BoundStatement hotelInsertBound =
        hotelInsertPrepared
            .bind()
            .setString("id", "STL102")
            .setString("name", "Hotel DEF")
            .setString("Phone", "012-345-6789");
    session.execute(hotelInsertBound);
  }
}
