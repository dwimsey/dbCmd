/**
 * Copyright 2016 David Wimsey - All rights reserved.
 * Author: David Wimsey <david@wimsey.us>
 */

package us.wimsey.dbcmd.sqlcmd;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.Properties;

import java.sql.Statement;
import org.json.JSONObject;

public class JDBCReaderFollowerThread extends Thread {
	public static Logger LOG = Logger.getLogger(JDBCReaderFollowerThread.class);

	private boolean _isDistributed;
	private boolean _keepGoing = true;

	// Configuration options
	int _minQueueSpaceRequiredForPoll; // Minimum space required in the _queue before a poll request will be made
	int _minRecordCountToPoll;         // The polling loop will not query the DB unless it intends to pull down at least this many records
	int _maxErrorSleepRetryTimeMS;     // When an error occurs in the connection/setup stage, wait no more than this long before trying again
	int _loopSleepTimeMinMS;           // Minimum amount of time to wait between polls of the DB when the last poll returned no rows
	int _loopSleepTimeMaxMS;           // Maximum amount of time to wait between polls of the DB when the last poll returned no rows

	int _minErrorSleepRetryTimeMS;     // When an error occurs in the connection/setup stage, wait at least this long before trying again
	int _maxQueuedRecords;             // Maximum number of records to hold in the _queue
	String _JDBCConnectionString = null;
	String _JDBCDriverClass = null;
	String _JDBCPollForItemsQuery = null;
	String _JDBCNextOffsetQuery = null;

	public JDBCReaderFollowerThread(Properties props) throws ClassNotFoundException {
		mapConfigSettings(props);

		if(_JDBCDriverClass != null) {
			// this will load the MySQL driver, each DB has its own driver
			Class.forName (_JDBCDriverClass);
		}
	}

	private void mapConfigSettings(Properties props) {
		_maxQueuedRecords = this.getInt(props.getProperty("MaxQueuedRecords", "500000"));
		_minQueueSpaceRequiredForPoll = this.getInt(props.getProperty("MinQueueSpaceRequiredForPoll", "250000"));
		_minRecordCountToPoll = this.getInt(props.getProperty("MinRecordCountToPoll", new Integer((_maxQueuedRecords < 1000 ? 1 : (_maxQueuedRecords/10))).toString()));
		_minErrorSleepRetryTimeMS = this.getInt(props.getProperty("MinErrorSleepRetryTimeMS", "500"));
		_maxErrorSleepRetryTimeMS = this.getInt(props.getProperty("MaxErrorSleepRetryTimeMS", "60000"));
		_loopSleepTimeMinMS = this.getInt(props.getProperty("LoopSleepTimeMinMS", "300000"));
		_loopSleepTimeMaxMS = this.getInt(props.getProperty("LoopSleepTimeMaxMS", "900000"));

		_JDBCConnectionString =  props.getProperty("ConnectionString");
		_JDBCDriverClass =  props.getProperty("DriverClass");
		_JDBCPollForItemsQuery = props.getProperty("JDBCPollForItemsQuery", "SELECT * FROM log_table WHERE id > ? ORDER BY id ASC LIMIT ?");
		_JDBCNextOffsetQuery = props.getProperty("JDBCNextIdQuery", "SELECT id FROM log_table ORDER BY id ASC LIMIT 1");
	}

	private static Integer getInt(Object o) {
		if(o == null) {
			throw new IllegalArgumentException("NULL object can not be converted to an Integer");
		}

		if(o instanceof Long) {
			return ((Long) o ).intValue();
		} else if (o instanceof Integer) {
			return (Integer) o;
		} else if (o instanceof Short) {
			return ((Short) o).intValue();
		} else if (o instanceof String) {
			return Integer.valueOf((String) o);
		} else {
			throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
		}
	}

	public void open() {
	}

	public void deactivate() {
		synchronized(this ) {
			this._keepGoing = false;
		}
	}

	public void activate() {
		synchronized(this ) {

			this._keepGoing = true;
		}
		this.start();
	}

	public void close() {
		synchronized(this ) {
			this._keepGoing = false;
		}
	}

	// This method determines how long to wait between polling requests when errors have occurred
	private int getErrorSleepDelay(int tryCount ) {
		int rVal = _minErrorSleepRetryTimeMS;
		while(tryCount > 0 ) {
			rVal *= 2;
			--tryCount;
		}
		if(rVal > _maxErrorSleepRetryTimeMS) {
			rVal = _maxErrorSleepRetryTimeMS;
		}
		return(rVal);
	}

	private int getIdleSleepDelay(int tryCount ) {
		int rVal = _loopSleepTimeMinMS;
		while(tryCount > 0 ) {
			rVal *= 2;
			--tryCount;
		}
		if(rVal > _loopSleepTimeMaxMS) {
			rVal = _loopSleepTimeMaxMS;
		}
		return(rVal);
	}

	/*
	    int _loopSleepTimeMinMS;           // Minimum amount of time to wait between polls of the DB when the last poll returned no rows
    int _loopSleepTimeMaxMS;           // Maximum amount of time to wait between polls of the DB when the last poll returned no rows

	 */
	// This method is used when we want to wait, but we also want the thread to be able to end without
	// hanging around until the timeout occurs.
	private void loopExitableWait(int delayInMilliseconds ) {
		try {
			Thread.sleep(delayInMilliseconds);
		} catch(java.lang.InterruptedException dontCare ) {
		}
	}

	long idLast = 0L;       // this should be populated by getting the last record in the system that was processed
	public void run() {
		Connection dbConnection = null;              // Holds our connection to DB
		PreparedStatement dbReadItemsCmd = null;     // Holds our prepared statement for the DB query
		boolean restartNeeded = true;                   // Used to flag if we need to (re)connect to DB, this flag is set when pretty much ANY error occurs, causing the connection to DB to be disconnected if open, cleaned up, and reconnected from scratch
		int dbRetries = 0;                           // The number of times this thread has tried to connect to DB and failed.  It is only reset to 0 after a query returns successfully and the expected columns can be mapped to indexes.
		int dbIdles = 0;
		try {
			// this try block is to ensure SocketIOServer gets cleaned up (finally)

			LOG.debug ("Starting JDBCReaderThread ...");
			while ( this._keepGoing ) {
				if ( restartNeeded ) {
					++dbRetries;
					if(dbConnection != null ) {
						if(dbReadItemsCmd != null ) {
							try {
								if(!dbReadItemsCmd.isClosed() ) {
									dbReadItemsCmd.close();
								}
							} catch(SQLException sqlEx ) {
								LOG.error ("Could not close JDBC prepared statement for connection restart (try: " + dbRetries + "): " + sqlEx.toString());
								restartNeeded = true;
								this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
								continue;
							} finally {
								dbReadItemsCmd = null;
							}
						}
						if(dbConnection != null ) {
							try {
								if(!dbConnection.isClosed() ) {
									dbConnection.close();
								}
							} catch(SQLException sqlEx ) {
								LOG.error ("Could not close JDBC connection for connection restart (try: " + dbRetries + "): " + sqlEx.toString());
								restartNeeded = true;
								this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
								continue;
							} finally {
								dbConnection = null;
							}
						}
						dbConnection = null;
					}
					try {
						// setup the connection with the DB.
						dbConnection = DriverManager.getConnection(this._JDBCConnectionString);
					} catch(SQLException sqlEx ) {
						LOG.error ("Could not open JDBC connection: " + this._JDBCConnectionString + ": " + sqlEx.toString());
						dbConnection = null;
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
						continue;
					}
					try {
						// preparedStatements can use variables and are more efficient
						dbReadItemsCmd = dbConnection.prepareStatement (_JDBCPollForItemsQuery);
					} catch(SQLException sqlEx ) {
						LOG.error ("Could not complete prepared statement setup for JDBC polling (try: " + dbRetries + "): " + sqlEx.toString());
						try {
							dbConnection.close();
							dbConnection = null;
						} catch(SQLException sqlExx ) {
							LOG.error ("Could not close JDBC connection when cleaning up failed prepared statement setup (try: " + dbRetries + "): " + sqlExx.toString());
						}
						dbReadItemsCmd = null;
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
						continue;
					}
					Statement getNextRecordOffset = null;
					try {
						getNextRecordOffset = dbConnection.createStatement();
						ResultSet nrs = getNextRecordOffset.executeQuery (_JDBCNextOffsetQuery);
						idLast = 0;
						if(nrs.next() ) {
							idLast = nrs.getLong(1);
							if(idLast > 0 ) {
								--idLast;
							}
							LOG.info ("Initial LastID value is: " + idLast);
						} else {
							LOG.error ("Initial LastID value lookup returned no results!");
						}
						nrs.close();
						if(idLast == 0 ) {
							LOG.error ("Initial LastID value is: 0");
							restartNeeded = true;
							this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
							continue;
						}
					} catch(SQLException sqlEx ) {
						LOG.error ("Could not complete prepared statement setup for JDBCSpout polling during activation (try: " + dbRetries + "): " + sqlEx.toString());
						try {
							dbConnection.close();
						} catch(SQLException sqlExx ) {
							LOG.error ("Could not close JDBC connection when cleaning up failed prepared statement setup in spout activation: " + sqlExx.toString());
						}
						dbConnection = null;
						dbReadItemsCmd = null;
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
						continue;
					} finally {
						try {
							getNextRecordOffset.close();
						} catch(SQLException sqlExx ) {
							LOG.error ("Could not close JDBC statement for initial JDBC index: " + sqlExx.toString());
						}
					}
					restartNeeded = false;
				}

				try {
					dbReadItemsCmd.setLong(1, idLast);
					dbReadItemsCmd.setLong(2, _maxQueuedRecords);
					LOG.debug ("Performing database polling query, attempting to fetch " + _maxQueuedRecords + " rows ...");
					ResultSet resultSet;
					try {
						resultSet = dbReadItemsCmd.executeQuery();
					} catch(SQLException sqlEx ) {
						LOG.error("Could not query JDBC: executeQuery failed: " + sqlEx.toString());
						LOG.error("Could not query JDBC: Query was: " + _JDBCPollForItemsQuery);
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
						continue;
					}

					if(resultSet != null ) {
						LOG.debug("Polling query completed, mapping column index values ...");
						ResultSetMetaData rsmd = resultSet.getMetaData();
						int numColumns = rsmd.getColumnCount();
						int nextRecordId_ColumnIndex = getResultSetFieldIndex ("id", resultSet);

						LOG.debug ("Mapping column index values complete.");
						long nextRecordId = idLast;
						int resultCount = 0;
						long processingTime = -1;
						long startTime = System.currentTimeMillis();

						try {
							if( resultSet.next()) {
								while (_keepGoing) {
									nextRecordId = resultSet.getLong(nextRecordId_ColumnIndex);
									JSONObject obj = new JSONObject();


									for (int i = 1; i < numColumns + 1; i++) {
										String column_name = rsmd.getColumnName(i);

										if (rsmd.getColumnType(i) == java.sql.Types.ARRAY) {
											obj.put(column_name, resultSet.getArray(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.BIGINT) {
											obj.put(column_name, resultSet.getLong(column_name));
										} else if (rsmd.getColumnType(i) == Types.INTEGER) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.BOOLEAN) {
											obj.put(column_name, resultSet.getBoolean(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.BLOB) {
											obj.put(column_name, resultSet.getBlob(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.DOUBLE) {
											obj.put(column_name, resultSet.getDouble(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.FLOAT) {
											obj.put(column_name, resultSet.getFloat(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.INTEGER) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.NVARCHAR) {
											obj.put(column_name, resultSet.getNString(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.VARCHAR) {
											obj.put(column_name, resultSet.getString(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.TINYINT) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.SMALLINT) {
											obj.put(column_name, resultSet.getInt(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.DATE) {
											obj.put(column_name, resultSet.getDate(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.TIME) {
											obj.put(column_name, resultSet.getTime(column_name));
										} else if (rsmd.getColumnType(i) == java.sql.Types.TIMESTAMP) {
											obj.put(column_name, resultSet.getTimestamp(column_name));
										} else {
											obj.put(column_name, resultSet.getObject(column_name));
										}
									}
									try {

										idLast = nextRecordId; // WE only update idLast if we successfully sent the message
										++resultCount;
										if (resultSet.next() == false) {
											// No more results to process, bail out
											break;
										}
									} catch (Exception ex) {
										int failureSleepTimeInMS = 500;
										LOG.error("Queue attempt failed: record id: " + nextRecordId + ": " + ex.getClass() + ": " + ex.getMessage());
										LOG.warn("Retry throttled, sleeping for " + failureSleepTimeInMS + "ms");
										try {
											Thread.sleep(failureSleepTimeInMS);
										} catch (InterruptedException e) {
											LOG.error(e);
										}
									}
								}
							}
							processingTime = System.currentTimeMillis() - startTime;
							// If we get to here, we consider the connection 'successful, and zero out 'retries'
							dbRetries = 0;
						} finally {
							resultSet.close();
						}
						if(!_keepGoing ) {
							// this will cause us to break out of the loop
							break;
						}
						// Yield processing for a brief moment, this essentially causes nothing more than a yield() to other threads as a curtesy
						int loopSleepTime = 1;
						if(resultCount > _minRecordCountToPoll ) {
							loopSleepTime = 1;
							dbIdles = 0;
							LOG.info ("Queued " + resultCount + " records in " + (processingTime / 1000.0) + "s (" + Math.round(resultCount / (processingTime/1000.0)) +  "/sec), last ID was: " + nextRecordId + ", sleeping for " + loopSleepTime/1000.0 + "ms");
						} else if(resultCount > 0 ) {
							// Less than 10k records, take a break
							loopSleepTime = getIdleSleepDelay(++dbIdles);
							LOG.info ("Queued " + resultCount + " records in " + (processingTime / 1000.0) + "s (" + Math.round(resultCount / (processingTime/1000.0)) +  "/sec), last ID was: " + nextRecordId + ", sleeping for " + loopSleepTime/1000.0 + "s");
						} else {
							loopSleepTime = getIdleSleepDelay(++dbIdles);
							LOG.info ("Too few results returned (" + resultCount + ") idling for " + (loopSleepTime / 1000.0 ) + " seconds");
						}
						this.loopExitableWait(loopSleepTime);
					} else {
						LOG.error ("No result set returned?!?!");
						restartNeeded = true;
						this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
					}
				} catch(SQLException sqlEx ) {
					LOG.error ("Could not close JDBC connection when processing query results: " + sqlEx.toString());
					restartNeeded = true;
					this.loopExitableWait(this.getErrorSleepDelay(dbRetries));
					continue;
				}
			}
			try {
				if(dbConnection != null ) {
					if(!dbConnection.isClosed() ) {
						dbConnection.close();
					}
				}
			} catch(SQLException sqlEx ) {
				LOG.error("Could not close JDBC connection when deactivating spout: " + sqlEx.toString());
			}
		} finally {

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
