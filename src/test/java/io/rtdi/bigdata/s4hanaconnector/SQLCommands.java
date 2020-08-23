package io.rtdi.bigdata.s4hanaconnector;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.Logger;

public class SQLCommands {
	
	private Connection conn;
	private Logger logger;

	public SQLCommands(Connection conn, Logger logger) {
		this.conn = conn;
		this.logger = logger;
	}

	public void executeScript(String scriptname) throws Exception {
		InputStream is = getClass().getClassLoader().getResourceAsStream(scriptname);
		if (is == null) {
			throw new SQLException("Cannot find the script with the file name " + scriptname);
		}
		try (BufferedReader isb = new BufferedReader(new InputStreamReader(is));) {
			try (Statement stmt = conn.createStatement();) {
				String line;
				while ((line = isb.readLine()) != null) {
					logger.debug(ellipses(line, 90));
					stmt.execute(line);
				}
				conn.commit();
			}
		}
	}

	public void dropTable(String schemaname, String tablename) throws SQLException {
		String identifier = "\"" + schemaname + "\".\"" + tablename + "\"";
		try (PreparedStatement stmt = conn.prepareStatement("drop table " + identifier + " cascade");) {
			logger.debug("dropping table " + identifier);
			stmt.execute();
		} catch (SQLException e) {
			if (e.getErrorCode() != 259) {
				throw e;
			} else {
				logger.debug("table " + identifier + " does not exist, nothing dropped");
			}
		}
		conn.commit();
	}
	
	private static String ellipses(String text, int maxlen) {
		if (text != null && text.length() > maxlen) {
			return text.substring(0, maxlen) + "...";
		} else {
			return text;
		}
	}
}
