/**
 * Copyright 2016 David Wimsey - All rights reserved.
 * Author: David Wimsey <david@wimsey.us>
 */

package us.wimsey.dbcmd;

import org.apache.commons.cli.*;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Level;
import org.apache.log4j.BasicConfigurator;
import us.wimsey.dbcmd.sqlcmd.JDBCResultSetReaderThread;
import us.wimsey.dbcmd.utils.*;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class dbCmd {
	private static final Logger LOG = Logger.getLogger(dbCmd.class);

	private static Properties props = null;
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure(); // Basic console output for log4j
		LogManager.getRootLogger().setLevel(Level.INFO);

		Options options = new Options();
		options.addOption("help", false, "Display help");
		options.addOption("defaults", false, "Show defaults file");
		options.addOption("nodefaults", false, "Ignore defaults file");
		options.addOption("properties", true, "Properties file to load for configuration properties");
		options.addOption("o", true, "Output format: csv, psv, tsv, json, sql, pretty");
		options.addOption("P", true, "Connection profile to use, if not default.");
		options.addOption("nc", false, "Don't display rows modified counts for cleaner output.");

		CommandLineParser parser = new DefaultParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
		} catch(UnrecognizedOptionException uoe) {
			System.err.println("Error with options specified: " + uoe.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "dbcmd", options );
			return;
		}
		if(line.hasOption("help") == true) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "dbcmd", options );
			return;
		}

		String propertiesFile = "dbcmd.properties";

		props = new Properties();
		if(line.hasOption("defaults") == true) {
			props = PropertyResources.loadPropertiesResourceFile(props, propertiesFile);
			StringWriter writer = new StringWriter();
			props.list(new PrintWriter(writer));
			System.out.println("sqlcmd defaults:");
			System.out.println(writer.getBuffer().toString());
			return;
		}

		if(line.hasOption( "nodefaults" ) == false) {
			Properties p = PropertyResources.loadPropertiesResourceFile(props, propertiesFile);
			if(p != null) {
				// If p is NULL, it means we couldn't load the file specified, so we leave props as it is, so defaults
				// pass through.
				// In this case however, p is not NULL, so update props with the new list
				props = p;
			}
			String home = System.getProperty("user.home");
			File propsFile = new File(home, "." + propertiesFile);
			props = PropertyResources.loadPropertiesFile(props, propsFile.getAbsolutePath());
		}

		if( line.hasOption( "properties" ) == true) {
			if (line.getOptionValue("properties") != null) {
				propertiesFile = line.getOptionValue("properties");
				props = PropertyResources.loadPropertiesFile(props, propertiesFile);
			}
		}


		if(props.getProperty("DriverClass") != null) {
			// This will attempt to load the drive class specified
			try {
				Class.forName(props.getProperty("DriverClass"));
			} catch (java.lang.ClassNotFoundException cnfEx) {
				LOG.error("Couldn't load '" + props.getProperty("DriverClass") + "' driver: " + cnfEx.toString());
				return;
			}
		}

		IResultSetWriter rsw = null;
		if(line.hasOption("o") == false) {
			// default to pretty-ish printed output
			rsw = new CSVResultSetWriter();
		} else {
			String outputFormat = line.getOptionValue("o", "pretty");
			switch(outputFormat) {
				case "csv":
				case "psv":
				case "tsv":
					rsw = new CSVResultSetWriter();
					break;
				case "json":
					rsw = new JSONResultSetWriter();
					break;
				case "sql":
					rsw = new SQLResultSetWriter();
					break;
				case "pretty":
					rsw = new PrettyResultSetWriter();
					break;
				default:
					throw new IllegalArgumentException("Unknown output format: " + outputFormat);
			}
		}

		String connectionProfile = null;
		if(props.containsKey("DefaultProfile") == true) {
			// We have a default profile specified in the config file, default to it
			connectionProfile = props.getProperty("DefaultProfile");
		}
		if(line.hasOption("P") == true) {
			// We have a profile specified on the command line, this will over ride anything in config files
			connectionProfile = line.getOptionValue("P", (connectionProfile != null ? connectionProfile : "(NULL)"));
		}

		// If we have a connectionProfile string, try to overlay the profile properties with the named profile
		if(connectionProfile != null) {
			// Map profile into properties
			if(props.containsKey(connectionProfile + ".ConnectionString") == true) {
				props.setProperty("ConnectionString", props.getProperty(connectionProfile + ".ConnectionString"));
			}
			if(props.containsKey(connectionProfile + ".DriverClass") == true) {
				props.setProperty("DriverClass", props.getProperty(connectionProfile + ".DriverClass"));
			}
		}

		if(line.hasOption("nc") == true || props.containsKey("NOCOUNT") == true) {
			props.setProperty("NOCOUNT", "true");
		}

		String[] blah = line.getArgs();
		StringBuilder qString = new StringBuilder();
		for(int i = 0; i < blah.length; i++) {
			qString.append(" " +  blah[i]);
		}

		String[] qStr = {qString.toString()};
		JDBCResultSetReaderThread rsThread;

		rsThread = new JDBCResultSetReaderThread(props, rsw, qStr);
		rsThread.start();
		rsThread.join();
		System.exit((int)rsThread.getValue());
	}
}
