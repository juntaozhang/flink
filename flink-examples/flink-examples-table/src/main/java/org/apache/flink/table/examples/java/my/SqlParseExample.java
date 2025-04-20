package org.apache.flink.table.examples.java.my;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class SqlParseExample {
    public static void main(String[] args) {
        String sql = "SELECT * FROM TABLE(f(a=>TABLE t PARTITION BY name,b=>1))";

        // Create a parser
        SqlParser parser = SqlParser.create(sql);

        try {
            // Parse the SQL statement
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("Parsed SQL: " + sqlNode.toSqlString(AnsiSqlDialect.DEFAULT));
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }
}
