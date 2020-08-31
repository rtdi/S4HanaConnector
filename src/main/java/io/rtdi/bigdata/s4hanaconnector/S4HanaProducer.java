package io.rtdi.bigdata.s4hanaconnector; 

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.enums.RowType;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.AvroNameEncoder;

/**
 * This is a trigger based S4Hana connector.
 * A connection to Hana is created and within this user the log table is created. The triggers on the
 * source database schema write the primary key of the changed record along with other data into this log table.
 * Another table keeps track of the read timestamps per producer. 
 *
 */
public class S4HanaProducer extends Producer<S4HanaConnectionProperties, S4HanaProducerProperties> {

	private Connection conn = null;
	private TopicHandler topic;
	private String username = null;
	private String sourcedbschema = null;
	/**
	 * The schema directory contains the BusinessObject for each schema name
	 */
	private Map<String, S4HanaTableMapping> schemadirectory = new HashMap<>();
	/**
	 * The table directory contains the same Business Object as the schema directory but for the mastertable being changed. 
	 * As a master table can be used in multiple schemas, the Map returns a List.
	 */
	private Map<String, List<S4HanaTableMapping>> tabledirectory = new HashMap<>();
	
	public S4HanaProducer(ProducerInstanceController instance) throws PropertiesException {
		super(instance);
		String sql = "select current_user from dummy";
		setConnection();
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				username = rs.getString(1);
			} else {
				throw new ConnectorRuntimeException("Selecting the current user from the database returned no records???", null, 
						"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Selecting the current user from the database failed?!?", e, 
					"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
		}
		sourcedbschema  = getConnectionProperties().getSourceSchema();
		logger.debug("Connected user is {} and source schema with the tables is {}", username, sourcedbschema);
	}
	
	private void setConnection() throws ConnectorRuntimeException {
		S4HanaConnectionProperties props = (S4HanaConnectionProperties) instance.getConnectionProperties();
		conn = S4HanaConnectorFactory.getDatabaseConnection(props);
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Failed to turn off autocommit in the JDBC driver", e, 
					"How could that happen?!?", "conn.setAutoCommit(false);");
		}
	}

	@Override
	public void startProducerChangeLogging() throws IOException {
		/*
		 * create trigger if not exists and the global log table
		 */
		String sql = null;
		try {
			if (conn == null || conn.isClosed()) {
				setConnection();
			}
			if (!S4HanaTableMapping.checktable("PKLOG", conn)) {
				
				sql = "create column table PKLOG ("
						+ "CHANGE_TS timestamp, "
						+ "SCHEMA_NAME nvarchar(256), "
						+ "CHANGE_TYPE varchar(1), "
						+ "PK1 nvarchar(256), "
						+ "PK2 nvarchar(256), "
						+ "PK3 nvarchar(256), "
						+ "PK4 nvarchar(256), "
						+ "PK5 nvarchar(256), "
						+ "PK6 nvarchar(256), "
						+ "TRANSACTIONID bigint, "
						+ "TRANSACTION_SEQ integer, "
						+ "TABLE_NAME nvarchar(256) )";
				try (PreparedStatement stmt = conn.prepareStatement(sql);) {
					stmt.execute();
				}
				logger.debug("Created the PKLOG table: {}", sql);
			}
			if (S4HanaTableMapping.checktable("DELTAINFO", conn)) {
				/* 
				 * This is needed for migration only. Prior to 2020-08 the change information was stored in the source,
				 * now it is in the transaction topic of Kafka.
				 */
				sql = "select top 1 delta_ts, transactionid from DELTAINFO where producername = ? order by delta_ts desc";
				try (PreparedStatement stmt = conn.prepareStatement(sql);) {
					stmt.setString(1, getProducerProperties().getName());
					try (ResultSet rs = stmt.executeQuery();) {
						if (rs.next()) {
							List<String> tables = getProducerProperties().getSourceSchemas();
							String transaction = rs.getString(2);
							for (String t : tables) {
								this.beginInitialLoadTransaction(transaction, t, 0);
								this.commitInitialLoadTransaction(0);
								logger.debug("Migrated the starting points of table {} from the DELTAINFO table into Kafka", t);
							}
						}
					}
				}
				sql = "delete from DELTAINFO where producername = ?";
				try (PreparedStatement stmt = conn.prepareStatement(sql);) {
					stmt.setString(1, getProducerProperties().getName());
					stmt.execute();
				}
				conn.commit();
			}
			List<String> sources = getProducerProperties().getSourceSchemas();
			if (sources != null) {
				for (String sourceschema : sources) {
					S4HanaTableMapping obj = schemadirectory.get(sourceschema);
					obj.createDeltaObjects();
				}
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Creating the Change Logging objects failed in the database", e, 
					"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
		}
	}

	@Override
	public void createTopiclist() throws IOException {
		TopicName t = TopicName.create(getProducerProperties().getTopicName());
		logger.debug("Create the topic for the topicname {} if it does not exist yet, actual Kafka topic name (encoded) is {}", t.getName(), t.getEncodedName());
		topic = getPipelineAPI().getTopicOrCreate(t, 1, (short) 1);
		List<String> sources = getProducerProperties().getSourceSchemas();
		if (sources != null) {
			for (String sourcetablename : sources) {
				SchemaHandler handler = getSchemaHandler(sourcetablename);
				if (handler != null) {
					addTopicSchema(topic, handler);
					logger.debug("Attached the schema {} to the topic", handler.getSchemaName().getName());
				}
			}
		}
	}

/*	@Override
	public String getLastSuccessfulSourceTransaction() throws IOException {
		String sql = "select transactionid from deltainfo where producername = ? order by delta_ts desc";
		try (PreparedStatement transactionlimitstmt = conn.prepareStatement(sql);) {
			transactionlimitstmt.setString(1, getProducerProperties().getName());
			ResultSet rs = transactionlimitstmt.executeQuery();
			if (rs.next()) {
				min_transactionid = rs.getLong(1);
				logger.debug("Starting point for delta is Hana transaction id \"{}\"", min_transactionid);
				return String.valueOf(min_transactionid);
			} else {
				min_transactionid = Long.MIN_VALUE;
				logger.debug("This producer never completed the initial loads");
				return null;
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Selecting the lower bound transaction limit failed", e, 
					"Any idea?", sql);
		}
	} */
	
	@Override
	public void startProducerCapture() throws IOException {
	}


	@Override
	public void restartWith(String lastsourcetransactionid) throws IOException {
	}

	@Override
	public long getPollingInterval() {
		return getProducerProperties().getPollInterval();
	}

	@Override
	public void closeImpl() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.info("Hana connection close failed - ignored", e);
			}
		}
	}

	@Override
	protected Schema createSchema(String sourceschema) throws SchemaException, IOException {
		try (S4HanaBrowse browser = new S4HanaBrowse(getConnectionController());) {
			S4HanaTableMapping obj = S4HanaTableMapping.readDefinition(username, sourcedbschema, sourceschema, conn, browser.getBusinessObjectDirectory());
			logger.debug("Mapping File with name {} read for Hana table {}", sourceschema, obj.getMastertable());
			schemadirectory.put(sourceschema, obj);
			List<S4HanaTableMapping> t = tabledirectory.get(obj.getMastertable());
			if (t == null) {
				t = new ArrayList<>();
				tabledirectory.put(obj.getMastertable(), t);
			}
			t.add(obj);
			return obj.getAvroSchema();
		}
	}

	private JexlRecord convert(ResultSet rs, Schema schema) throws SQLException, ConnectorRuntimeException, SchemaException {
		JexlRecord r = new JexlRecord(schema);
		for (int i=3; i<= rs.getMetaData().getColumnCount(); i++) {
			String columnname = rs.getMetaData().getColumnLabel(i);
			String avrofieldname = AvroNameEncoder.encodeName(columnname);
			int datatype = rs.getMetaData().getColumnType(i);
			JDBCType t = JDBCType.valueOf(datatype);
			switch (t) {
			case BIGINT:
				r.put(avrofieldname, rs.getLong(i));
				break;
			case BINARY:
				r.put(avrofieldname, rs.getBytes(i));
				break;
			case BLOB:
				r.put(avrofieldname, rs.getBytes(i));
				break;
			case BOOLEAN:
				r.put(avrofieldname, rs.getBoolean(i));
				break;
			case CHAR:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			case CLOB:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			case DATE:
				r.put(avrofieldname, rs.getDate(i));
				break;
			case DECIMAL:
				r.put(avrofieldname, rs.getBigDecimal(i));
				break;
			case DOUBLE:
				r.put(avrofieldname, rs.getDouble(i));
				break;
			case FLOAT:
				r.put(avrofieldname, rs.getFloat(i));
				break;
			case INTEGER:
				r.put(avrofieldname, rs.getInt(i));
				break;
			case LONGNVARCHAR:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			case LONGVARBINARY:
				r.put(avrofieldname, rs.getBytes(i));
				break;
			case LONGVARCHAR:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			case NCHAR:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			case NCLOB:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			case NVARCHAR:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			case REAL:
				r.put(avrofieldname, rs.getFloat(i));
				break;
			case SMALLINT:
				r.put(avrofieldname, rs.getInt(i));
				break;
			case TIME:
				r.put(avrofieldname, rs.getTime(i));
				break;
			case TIMESTAMP:
				r.put(avrofieldname, rs.getTimestamp(i));
				break;
			case TIMESTAMP_WITH_TIMEZONE:
				r.put(avrofieldname, rs.getTimestamp(i));
				break;
			case TIME_WITH_TIMEZONE:
				r.put(avrofieldname, rs.getTimestamp(i));
				break;
			case TINYINT:
				r.put(avrofieldname, rs.getInt(i));
				break;
			case VARBINARY:
				r.put(avrofieldname, rs.getBytes(i));
				break;
			case VARCHAR:
				r.put(avrofieldname, trim(rs.getString(i)));
				break;
			default:
				throw new ConnectorRuntimeException("The select statement returns a datatype the connector cannot handle", null, 
						"Please create an issue", rs.getMetaData().getColumnName(i) + ":" + t.getName());
			}
			if (rs.wasNull()) {
				r.put(avrofieldname, null);
			}
		}
		return r;
	}

	private String trim(String value) {
		if (value == null || value.length() == 0) {
			return null;
		} else {
			return value;
		}
	}
	
	private long getMaxTransactionId(long min_transactionid) throws ConnectorRuntimeException {
		long max_transactionid = 0;
		String sql = "select least(max_log, min_active) from\r\n" + 
				"(select ifnull(max(transactionid), 9223372036854775807) max_log from pklog),\r\n" + 
				"(select ifnull(min(update_transaction_id-1), 9223372036854775807) min_active from m_transactions where update_transaction_id > 0)";
		try (PreparedStatement transactionlimitstmt = conn.prepareStatement(sql);) {
			ResultSet rs = transactionlimitstmt.executeQuery();
			if (rs.next()) {
				max_transactionid = rs.getLong(1);
				if (max_transactionid == 9223372036854775807L) {
					max_transactionid = min_transactionid;
				}
			} else {
				max_transactionid = min_transactionid; // given above sql this cannot happen
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Selecting the upper bound transaction limit failed", e, 
					"Any idea?", sql);
		}
		logger.debug("Highest committed transaction id in Hana is \"{}\"", max_transactionid);
		return max_transactionid;
	}

	@Override
	public String getCurrentTransactionId() throws ConnectorRuntimeException {
		long current_transactionid = 0;
		String sql = "select least(max_log, min_active) from\r\n" + 
				"(select ifnull(max(transactionid), 0) max_log from pklog),\r\n" + 
				"(select ifnull(min(update_transaction_id-1), 9223372036854775807) min_active from m_transactions where update_transaction_id > 0)";
		try (PreparedStatement transactionlimitstmt = conn.prepareStatement(sql);) {
			ResultSet rs = transactionlimitstmt.executeQuery();
			if (rs.next()) {
				current_transactionid = rs.getLong(1);
			} else {
				current_transactionid = Long.MAX_VALUE; // given above sql this cannot happen
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Selecting the current transaction id failed", e, 
					"Any idea?", sql);
		}
		logger.debug("Current transaction id in Hana is \"{}\"", current_transactionid);
		return String.valueOf(current_transactionid);
	}

	@Override
	public String poll(String from_transaction) throws IOException {
		long min_transactionid = Long.valueOf(from_transaction);
		long max_transactionid = getMaxTransactionId(min_transactionid);
		String sql = null;
		
		if (min_transactionid != max_transactionid) { // If Hana has not processed a single record anywhere, no need to check for data
			logger.debug("Reading change data from Hana transaction id \"{}\" to transaction id \"{}\"", min_transactionid, max_transactionid);
			try {
				sql = "select distinct table_name from PKLOG where schema_name = ? and transactionid > ? and transactionid <= ?";
				Set<S4HanaTableMapping> impacted = new HashSet<>();
				try (PreparedStatement logtablesstmt = conn.prepareStatement(sql);) {
					logtablesstmt.setString(1, sourcedbschema);
					logtablesstmt.setLong(2, min_transactionid);
					logtablesstmt.setLong(3, max_transactionid);
					
					/*
					 * Read all tables that got changed and translate that to the master tables to be read.
					 * For example the item table got changed and hence the order object has to be recreated.
					 */
					try (ResultSet logtablesrs = logtablesstmt.executeQuery();) {
						while (logtablesrs.next()) {
							String changetable = logtablesrs.getString(1);
							List<S4HanaTableMapping> t = tabledirectory.get(changetable);
							if (t != null) {
								impacted.addAll(t);
							}
						}
					}
				}
				if (impacted.size() > 0) {
					logger.debug("Found changes for mappings \"{}\"", impacted.toString());
					beginDeltaTransaction(String.valueOf(max_transactionid), instance.getInstanceNumber());
					for (S4HanaTableMapping obj : impacted) {
						String currentschema = obj.getName();
						sql = obj.getDeltaSelect();
						try (PreparedStatement stmt = conn.prepareStatement(sql);) {
							stmt.setLong(1, min_transactionid);
							stmt.setLong(2, max_transactionid);
							try (ResultSet rs = stmt.executeQuery(); ) {
								while (rs.next()) {
									JexlRecord r = convert(rs, obj.getAvroSchema());
					    			RowType rowtype;
					    			switch (rs.getString(1)) {
					    			case "D": 
					    				rowtype = RowType.DELETE;
					    				break;
					    			default: 
					    				rowtype = RowType.UPSERT;
					    			}
					    			addRow(topic, null, getSchema(currentschema), r, rowtype, null, getProducerProperties().getName());
					    			logger.debug("Sending row {}", r.toString());
								}
							}
						}
					}
					
					commitDeltaTransaction();
					conn.commit();
				}
				logger.debug("Moved min transaction id to \"{}\" as new starting point", max_transactionid);
				return String.valueOf(max_transactionid);
			} catch (SQLException e) {
				abortTransaction();
				throw new ConnectorRuntimeException("Selecting the changes ran into an error", e, "Any idea?", sql);
			} catch (SchemaException e) {
				abortTransaction();
				throw new ConnectorRuntimeException("Selecting the changes ran into an error with the schema", e, 
						"Any idea?", null);
			}
		} else {
			return from_transaction;
		}
	}

/*	private void updateDeltaInfo(long max_transactionid) throws ConnectorRuntimeException {
		String sql = "insert into deltainfo (delta_ts, producername, transactionid) values (now(), ?, ?)";
		try (PreparedStatement transactionlimitstmt = conn.prepareStatement(sql);) {
			transactionlimitstmt.setString(1, getProducerProperties().getName());
			transactionlimitstmt.setLong(2, max_transactionid);
			transactionlimitstmt.execute();
			min_transactionid = max_transactionid;
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Inserting the new successful delta interval into the DELTAINFO table failed", e, 
					"Any idea?", sql);
		}
	} */

	/**
	 * Delete all old data from PKLOG and DELTAINFO
	 */
	public void executePeriodicTask() throws ConnectorRuntimeException {
		// The condition transactionid <> 0 is important as this acts as the indicator for initial loads
		/* String sql = "delete from deltainfo where delta_ts < add_days(now(), -7) and transactionid <> 0";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.execute();
			logger.debug("Deleted outdated data from DELTAINFO");
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Deleting outdated data from the DELTAINFO table failed", e, 
					"Any idea?", sql);
		} finally {
			try {
				conn.commit();
			} catch (SQLException e) {
				logger.error("Failed ot commit", e);
			}
		} */
		String sql = "delete from pklog where CHANGE_TS < add_days(now(), -7)";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.execute();
			logger.debug("Deleted outdated data from PKLOG");
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Deleting outdated data from the PKLOG table failed", e, 
					"Any idea?", sql);
		} finally {
			try {
				conn.commit();
			} catch (SQLException e) {
				logger.error("Failed ot commit", e);
			}
		}

	}

	@Override
	public List<String> getAllSchemas() {
		ArrayList<String> l = new ArrayList<>();
		l.addAll(schemadirectory.keySet());
		return l;
	}

	@Override
	public long executeInitialLoad(String schemaname, String transactionid) throws IOException {
		S4HanaTableMapping obj = schemadirectory.get(schemaname);
		logger.debug("Initial load for mapping \"{}\" is about to start", schemaname);
		String sql = obj.getInitialSelect();
		SchemaHandler schemahandler = getSchema(obj.getName());
		Schema schema = null;
		try (PreparedStatement stmt = conn.prepareStatement(sql); ) {
			schema = obj.getAvroSchema();
			beginInitialLoadTransaction(transactionid, schemaname, instance.getInstanceNumber());
			long rowcount = 0L;
			try (ResultSet rs = stmt.executeQuery();) {
				while (rs.next()) {
					JexlRecord r = convert(rs, schema);
					addRow(topic,
							null,
							schemahandler,
							r,
							RowType.INSERT,
							null,
							getProducerProperties().getName());
					rowcount++;
				}
			}
			commitInitialLoadTransaction(rowcount);
			logger.debug("Initial load for mapping \"{}\" is completed, loaded {} rows", schemaname, rowcount);
			return rowcount;
		} catch (SQLException e) {
			abortTransaction();
			throw new ConnectorRuntimeException("Executing the initial load SQL failed with SQL error", e, 
					"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
		} catch (SchemaException e) {
			abortTransaction();
			throw new ConnectorRuntimeException("SchemaException thrown when assigning the values", e, 
					null, schema.toString());
		}
	}
}
