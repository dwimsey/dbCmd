package us.wimsey.dbcmd.utils;

import org.json.JSONObject;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Created by dwimsey on 1/17/16.
 */
public class JSONResultSetWriter implements IResultSetWriter {
	private PrintStream outputStream = null;

	public PrintStream setOutputStream(PrintStream outStream) {
		PrintStream previousStream = outputStream;
		outputStream = outStream;
		return(previousStream);
	}


	public long outputResultSet(ResultSet resultSet) throws SQLException {
		return(outputResultSet(resultSet, Long.MAX_VALUE));
	}

	public long outputResultSet(ResultSet resultSet, long limit) throws SQLException {
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int numColumns = rsmd.getColumnCount();
		long resultCount = 0;
		while (resultSet.next()) {
			JSONObject obj = new JSONObject();


			for (int i = 1; i < numColumns + 1; i++) {
				String column_name = rsmd.getColumnName(i);

				if (rsmd.getColumnType(i) == Types.ARRAY) {
					obj.put(column_name, resultSet.getArray(column_name));
				} else if (rsmd.getColumnType(i) == Types.BIGINT) {
					obj.put(column_name, resultSet.getLong(column_name));
				} else if (rsmd.getColumnType(i) == Types.INTEGER) {
					obj.put(column_name, resultSet.getInt(column_name));
				} else if (rsmd.getColumnType(i) == Types.BOOLEAN) {
					obj.put(column_name, resultSet.getBoolean(column_name));
				} else if (rsmd.getColumnType(i) == Types.BLOB) {
					obj.put(column_name, resultSet.getBlob(column_name));
				} else if (rsmd.getColumnType(i) == Types.DOUBLE) {
					obj.put(column_name, resultSet.getDouble(column_name));
				} else if (rsmd.getColumnType(i) == Types.FLOAT) {
					obj.put(column_name, resultSet.getFloat(column_name));
				} else if (rsmd.getColumnType(i) == Types.INTEGER) {
					obj.put(column_name, resultSet.getInt(column_name));
				} else if (rsmd.getColumnType(i) == Types.NVARCHAR) {
					obj.put(column_name, resultSet.getNString(column_name));
				} else if (rsmd.getColumnType(i) == Types.VARCHAR) {
					obj.put(column_name, resultSet.getString(column_name));
				} else if (rsmd.getColumnType(i) == Types.TINYINT) {
					obj.put(column_name, resultSet.getInt(column_name));
				} else if (rsmd.getColumnType(i) == Types.SMALLINT) {
					obj.put(column_name, resultSet.getInt(column_name));
				} else if (rsmd.getColumnType(i) == Types.DATE) {
					obj.put(column_name, resultSet.getDate(column_name));
				} else if (rsmd.getColumnType(i) == Types.TIME) {
					obj.put(column_name, resultSet.getTime(column_name));
				} else if (rsmd.getColumnType(i) == Types.TIMESTAMP) {
					obj.put(column_name, resultSet.getTimestamp(column_name));
				} else {
					obj.put(column_name, resultSet.getObject(column_name));
				}
			}
			outputStream.println(obj.toString() + ",");
			++resultCount;
			if(resultCount > limit) {
				return(resultCount);
			}
			if (resultSet.next() == false) {
				// No more results to process, bail out
				break;
			}
		}
		return(resultCount);
	}
}
