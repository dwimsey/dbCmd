package us.wimsey.dbcmd.utils;

import java.io.PrintStream;
import java.sql.ResultSet;

/**
 * Created by dwimsey on 1/17/16.
 */
public class SQLResultSetWriter implements IResultSetWriter {
	private PrintStream outputStream = null;

	public PrintStream setOutputStream(PrintStream outStream) {
		PrintStream previousStream = outputStream;
		outputStream = outStream;
		return(previousStream);
	}

	public long outputResultSet(ResultSet resultSet) {
		return(outputResultSet(resultSet, Long.MAX_VALUE));
	}

	public long outputResultSet(ResultSet results, long limit) {
		return(0L);
	}
}
