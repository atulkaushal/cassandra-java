package com.github.atulkaushal.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.select.Select;

/**
 * The Class QueryBuilderExample.
 *
 * @author Atul
 */
public class QueryBuilderExample {

  /**
   * The main method.
   *
   * @param args the arguments
   */
  public static void main(String args[]) {

    CqlSession session = CqlSession.builder().build();
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
   * @param session the session
   * @return the result set
   */
  private static ResultSet fetchRow(CqlSession session) {
    Select hotelSelect =
        QueryBuilder.selectFrom("hotel", "hotels")
            .all()
            .whereColumn("id")
            .isEqualTo(literal("STL103"));
    SimpleStatement hotelSelectStatement = hotelSelect.build();
    return session.execute(hotelSelectStatement);
  }

  /**
   * Insert row.
   *
   * @param session the session
   */
  private static void insertRow(CqlSession session) {
    Insert hotelInsert =
        insertInto("hotel", "hotels")
            .value("id", literal("STL104"))
            .value("name", literal("Hotel JKL"))
            .value("phone", literal("000-111-2222"));
    SimpleStatement hotelInsertStatement = hotelInsert.build();
    session.execute(hotelInsertStatement);
  }
}
