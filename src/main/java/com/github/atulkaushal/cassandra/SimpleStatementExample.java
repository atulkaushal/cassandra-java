package com.github.atulkaushal.cassandra;

import java.text.SimpleDateFormat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.TraceEvent;

/**
 * The Class SimpleStatementExample.
 *
 * @author Atul
 */
public class SimpleStatementExample {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String[] args) {
    CqlSession session = CqlSession.builder().build();
    insertRow(session);

    ResultSet hotelSelectResult = fetchRow(session);

    printRow(hotelSelectResult);

    // result metadata
    System.out.println(hotelSelectResult);
    System.out.println(hotelSelectResult.wasApplied());
    System.out.println(hotelSelectResult.getExecutionInfo());
    System.out.println(hotelSelectResult.getExecutionInfo().getIncomingPayload());
    System.out.println(hotelSelectResult.getExecutionInfo().getQueryTrace());

    System.out.println(hotelSelectResult.getExecutionInfo().getQueryTrace().getStartedAt());
    SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
    for (TraceEvent event : hotelSelectResult.getExecutionInfo().getQueryTrace().getEvents()) {
      System.out.printf(
          "%42s | %12s | %10s\n",
          event.getActivity(), dateFormat.format((event.getTimestamp())), event.getSourceAddress());
    }

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
   * @param session the session
   * @return the result set
   */
  private static ResultSet fetchRow(CqlSession session) {
    SimpleStatement reservationSelect =
        SimpleStatement.builder("SELECT * FROM hotel.hotels WHERE id=?")
            .addPositionalValue("STL101")
            .setTracing(true)
            .build();
    ResultSet hotelSelectResult = session.execute(reservationSelect);
    return hotelSelectResult;
  }

  /**
   * Insert row.
   *
   * @param session the session
   */
  private static void insertRow(CqlSession session) {
    SimpleStatement reservationInsert =
        SimpleStatement.builder("INSERT INTO hotel.hotels (id,name,phone) VALUES (?, ?, ?)")
            .addPositionalValue("STL101")
            .addPositionalValue("Hotel ABC")
            .addPositionalValue("123-456-7890")
            .build();
    session.execute(reservationInsert);
  }
}
