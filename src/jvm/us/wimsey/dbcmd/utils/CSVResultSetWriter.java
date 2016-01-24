package us.wimsey.dbcmd.utils;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dwimsey on 1/17/16.
 */
public class CSVResultSetWriter implements IResultSetWriter {
	private PrintStream outputStream = null;

	public PrintStream setOutputStream(PrintStream outStream) {
		PrintStream previousStream = outputStream;
		outputStream = outStream;
		return(previousStream);
	}

	public long outputResultSet(ResultSet resultSet) throws SQLException {
		return (outputResultSet(resultSet, Long.MAX_VALUE));
	}

	public long outputResultSet(ResultSet resultSet, long limit) throws SQLException {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int numColumns = rsmd.getColumnCount();
		long resultCount = 0;
		String val = null;
		String column_name = null;

		for (int i = 1; i < numColumns + 1; i++) {
			column_name = rsmd.getColumnName(i);
			if (column_name != null) {
				if (column_name.isEmpty() == false) {
					if (column_name.contains("\"") == true) {
						column_name = "\"" + column_name.replace("\"", "\\\"") + "\"";
					} else if (column_name.contains(",") == true) {
						column_name = "\"" + column_name + "\"";
					}
				} else {
					column_name = "coledx_" + i;
				}
			} else {
				column_name = "colidx_" + i;
			}
			if (i > 1) {
				outputStream.print("," + column_name);
			} else {
				outputStream.print(column_name);
			}
		}
		outputStream.println("");

		while (resultSet.next()) {
			for (int i = 1; i < numColumns + 1; i++) {
				switch (rsmd.getColumnType(i)) {
					case Types.ARRAY:
					case Types.BIGINT:
					case Types.INTEGER:
					case Types.BOOLEAN:
					case Types.BLOB:
					case Types.DOUBLE:
					case Types.FLOAT:
					case Types.NVARCHAR:
					case Types.VARCHAR:
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.DATE:
					case Types.TIME:
					case Types.TIMESTAMP:
					default:
						val = resultSet.getString(i);
						if (val.contains("\"") == true) {
							val = "\"" + val.replace("\"", "\\\"") + "\"";    // Double quote the string, it has quotes in it.  This effectively handles comma's as well since the entire string is quoted in the end
						} else if (val.contains(",") == true) {
							val = "\"" + val + "\"";            // Quote the string, it has a comma in it
						}
						break;
				}
				if (i > 1) {
					outputStream.print("," + val);
				} else {
					outputStream.print(val);
				}
			}
			outputStream.println("");
			++resultCount;
			if (resultCount > limit) {
				outputStream.flush();
				return (resultCount);
			}
			if (resultSet.next() == false) {
				// No more results to process, bail out
				break;
			}
		}
		outputStream.flush();
		return (resultCount);
	}
}
