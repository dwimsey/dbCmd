package us.wimsey.dbcmd.utils;

import java.io.PrintStream;
import java.sql.ResultSet;

/**
 * Created by dwimsey on 1/17/16.
 */
public interface IResultSetWriter {
	public long outputResultSet(ResultSet results, long limit) throws Exception;
	public long outputResultSet(ResultSet results) throws Exception;

	public PrintStream setOutputStream(PrintStream outStream);
}
