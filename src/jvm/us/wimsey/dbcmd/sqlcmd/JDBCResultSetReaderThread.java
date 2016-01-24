/**
 * Copyright 2016 David Wimsey - All rights reserved.
 * Author: David Wimsey <david@wimsey.us>
 */

package us.wimsey.dbcmd.sqlcmd;

import org.apache.log4j.Logger;
import us.wimsey.dbcmd.utils.IResultSetWriter;

import java.io.PrintStream;
import java.sql.*;
import java.util.Properties;

public class JDBCResultSetReaderThread extends Thread {
	public static Logger LOG = Logger.getLogger(JDBCResultSetReaderThread.class);

	String _JDBCConnectionString = null;
	String _JDBCDriverClass = null;
	String[] _QueryStrings = null;
	IResultSetWriter _ResultSetWriter = null;
	boolean _NOCOUNTs = false;

	public JDBCResultSetReaderThread(Properties props, IResultSetWriter resultSetWriter, String[] queryStrings) throws ClassNotFoundException {
		_ResultSetWriter = resultSetWriter;
		_QueryStrings = queryStrings;

		mapConfigSettings(props);

		if(_JDBCDriverClass != null) {
			// this will load the MySQL driver, each DB has its own driver
			Class.forName (_JDBCDriverClass);
		}
	}

	private void mapConfigSettings(Properties props) {
		_JDBCConnectionString =  props.getProperty("ConnectionString");
		_JDBCDriverClass =  props.getProperty("DriverClass");
		String tmpStr = props.getProperty("NOCOUNT"); // If true, hide row counts
		if(tmpStr != null) {
			if (("true".equals(tmpStr) == true) || ("yes".equals(tmpStr) == true) || ("1".equals(tmpStr) == true)) {
				_NOCOUNTs = true;
			} else {
				_NOCOUNTs = false;
			}
		}
	}

	private volatile long returnValue = 0;

	public long getValue() {
		return returnValue;
	}

	private PrintStream _outputStream = System.out;
	private PrintStream _errorStream = System.err;
	public void run() {
		returnValue = 0;
		int resultCount = 0;
		Connection dbConnection = null;              // Holds our connection to DB
		PreparedStatement dbReadItemsCmd = null;     // Holds our prepared statement for the DB query
		LOG.debug("Starting JDBCReaderThread ...");
		boolean hasResultSet;
		int updateValue;

		for(int queryIndex = 0; queryIndex < _QueryStrings.length; queryIndex++) {
			try {
				dbConnection = DriverManager.getConnection(this._JDBCConnectionString);


				try {
					dbReadItemsCmd = dbConnection.prepareStatement(_QueryStrings[queryIndex]);

					// This only handles the first result set returned
					try {
						hasResultSet = dbReadItemsCmd.execute();
					} catch (SQLException sx) {
						_errorStream.println("----------------------------------------------------------------------------");
						_errorStream.println("Statement execution failed: " + sx.getMessage() + ":");
						_errorStream.println(_QueryStrings[queryIndex]);
						continue;
					}
					// This loop continues until hasResultSet == false and dbReadItemsCmd.getUpdateCount() == -1, which
					// means no more results to read, at which point we return the last updated count or number of
					// rows selected in the batch.
					while(true) {
						if (hasResultSet) {
							++resultCount;
							// This should never be null since we're being told we have a result set by the execute()/getMoreResults method.
							ResultSet resultSet = dbReadItemsCmd.getResultSet();
							long processingTime = -1;
							long startTime = System.currentTimeMillis();
							_ResultSetWriter.setOutputStream(_outputStream);
							returnValue = _ResultSetWriter.outputResultSet(resultSet);
							processingTime = System.currentTimeMillis() - startTime;
							resultSet.close();
							LOG.debug("Processed " + returnValue + " records in " + (processingTime / 1000.0) + "s (" + Math.round(returnValue / (processingTime / 1000.0)) + "/sec)");
						} else {
							// Query did not return a result set, look for an update count
							updateValue = dbReadItemsCmd.getUpdateCount();
							if(updateValue == -1) {
								// -1 means there are no move result sets or update counts from the batch, we're done.
								// break the loop
								break;
							}

							// Another rows effected count, continue processing.
							++resultCount;
							returnValue = updateValue;
							if(_NOCOUNTs == false) {
								// Only display rowcounts if _NOCOUNTs is false
								_outputStream.println(updateValue + " rows effected.");
							}
						}
						// This returns true if there is another result set waiting next, otherwise false.  If  false, on
						// the next loop we'll check its update value for -1 indicating no more items and break from there.
						hasResultSet = dbReadItemsCmd.getMoreResults();
					}

					dbReadItemsCmd.close();

				} catch (SQLException se) {
					se.printStackTrace();
				}
				if (!dbConnection.isClosed()) {
					dbConnection.close();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private int getResultSetFieldIndex(String desiredColumnName, ResultSet resultSet ) throws SQLException {
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		for(int i = 1; i <= columnCount; i++ ) {
			if(metaData.getColumnName(i).equals(desiredColumnName)) {
				return(i);
			}
		}
		throw new SQLException ("Column name requested could not be found in ResultSet: " + desiredColumnName);
	}
}
