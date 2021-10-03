package io.rtdi.bigdata.s4hanaconnector; 

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.avro.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;

import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.SchemaHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicHandler;
import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.AvroNameEncoder;
import io.rtdi.bigdata.kafka.avro.RowType;
import io.rtdi.bigdata.kafka.avro.RuleResult;

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
		setConnection();
		try {
			username = conn.getSchema();
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Getting the current user for the database failed?!?", e, 
					null, null);
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
				logger.info("Created the PKLOG table: {}", sql);
				addOperationLogLine("Created the PKLOG table");
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
								this.commitInitialLoadTransaction();
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
		logger.info("Create the topic for the topicname {} if it does not exist yet, actual Kafka topic name (encoded) is {}", t.getName(), t.getEncodedName());
		topic = getPipelineAPI().getTopicOrCreate(t, 1, (short) 1);
		List<String> sources = getProducerProperties().getSourceSchemas();
		if (sources != null) {
			for (String sourcetablename : sources) {
				SchemaHandler handler = getSchemaHandler(sourcetablename);
				if (handler != null) {
					addTopicSchema(topic, handler);
					logger.info("Attached the schema {} to the topic", handler.getSchemaName().getName());
				}
			}
		}
	}

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
				r.put(avrofieldname, rs.getString(i));
				break;
			case CLOB:
				r.put(avrofieldname, rs.getString(i));
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
				r.put(avrofieldname, rs.getString(i));
				break;
			case LONGVARBINARY:
				r.put(avrofieldname, rs.getBytes(i));
				break;
			case LONGVARCHAR:
				r.put(avrofieldname, rs.getString(i));
				break;
			case NCHAR:
				r.put(avrofieldname, rs.getString(i));
				break;
			case NCLOB:
				r.put(avrofieldname, rs.getString(i));
				break;
			case NVARCHAR:
				r.put(avrofieldname, rs.getString(i));
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
				r.put(avrofieldname, rs.getString(i));
				break;
			default:
				throw new ConnectorRuntimeException("The select statement returns a datatype the connector cannot handle", null, 
						"Please create an issue", rs.getMetaData().getColumnName(i) + ":" + t.getName());
			}
			if (rs.wasNull()) {
				/*
				 * Above code might have e.g. put a 0L into the record even if it was null really.
				 * Hence validate it was 0L or a null value
				 */
				r.put(avrofieldname, null);
			}
		}
		return r;
	}
	
	private long getMaxTransactionId(long min_transactionid) throws ConnectorRuntimeException {
		long max_transactionid = 0;
		/* 
		 * In order to read the data in commit order, the data is processed up to the first 
		 * uncommitted transaction and not further.
		 * This has one issue, what if a session starts a transaction and it is not committed or
		 * rolled back for many hours or days? This cannot happen with SAP ABAP transactions.
		 * Hence the fall back is to consider only in-flight transactions younger than two hours.
		 */
		String sql = "select least(max_log, min_active), start_time from\r\n" + 
				"(select ifnull(max(transactionid), 9223372036854775807) max_log from pklog),\r\n" + 
				"(select\r\n"
				+ "  ifnull(min(update_transaction_id-1), 9223372036854775807) min_active,\r\n"
				+ "  min(start_time) start_time\r\n"
				+ "from m_transactions where update_transaction_id > 0 and start_time > add_seconds(now(), -7200))";
		try (PreparedStatement transactionlimitstmt = conn.prepareStatement(sql);) {
			ResultSet rs = transactionlimitstmt.executeQuery();
			if (rs.next()) {
				max_transactionid = rs.getLong(1);
				if (max_transactionid == 9223372036854775807L) {
					max_transactionid = min_transactionid;
				}
				Timestamp mintransactiondate = rs.getTimestamp(2);
				if (mintransactiondate != null) {
					long diff = System.currentTimeMillis() - mintransactiondate.getTime();
					/*
					 * An open transaction for a few milliseconds is normal. But longer than one minute should 
					 * start notifying the user. 
					 */
					if (diff > 60000) {
						addOperationLogLine("A Hana transaction is open since " 
								+ String.valueOf(diff/1000L)
								+ " seconds, hence there is increased latency in the replication",
								null, RuleResult.WARN);
						logger.info("A Hana transaction is open since {} seconds, hence "
								+ "there is increased latency in the replication", diff/1000L);
					} else if (diff > 5000) {
						addOperationLogLine("A Hana transaction is open since " 
								+ String.valueOf(diff/1000L) + " seconds, hence there is increased "
										+ "latency in the replication");
					}
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
		addOperationLogLine("Begin of iteration from " + String.valueOf(min_transactionid) + " to " + String.valueOf(max_transactionid));
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
					addOperationLogLine("Found relevant change records in PKLOG");
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
					addOperationLogLine("Iteration ended, data sent");
				} else {
					addOperationLogLine("Iteration ended, no changes in PKLOG for the producer tables");
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
			addOperationLogLine("Iteration ended");
			return from_transaction;
		}
	}

	/**
	 * Delete all old data from PKLOG and DELTAINFO
	 */
	public void executePeriodicTask() throws ConnectorRuntimeException {
		String sql = "delete from pklog where CHANGE_TS < add_days(now(), -7)";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			int d = stmt.executeUpdate();
			addOperationLogLine("Deleting " + String.valueOf(d) + "rows from PKLOG which were older than 7 days");
			logger.info("Deleted outdated data from PKLOG");
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
		/*
		 * The initial load process is to
		 * a) Get the partition names of the table available in the source
		 * b) Create a task for each partition or one task for the entire table if it is not partitioned 
		 * c) Create a worker pool to load up to 10 partitions in parallel. 
		 *    If one partition fails, stop the initial load. 
		 */
		addOperationLogLine("Initial load for " + schemaname + " started");
		S4HanaTableMapping obj = schemadirectory.get(schemaname);
		String sql = "select partition from m_cs_partitions where schema_name= ? and table_name = ?";
		List<InitialLoadTask> tasks = new ArrayList<>();
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, sourcedbschema);
			stmt.setString(2, schemaname);
			try (ResultSet rs = stmt.executeQuery();) {
				while (rs.next()) {
					InitialLoadTask t = new InitialLoadTask(obj, transactionid, rs.getInt(1));
					tasks.add(t);
				}
			}
			if (tasks.size() == 0) {
				InitialLoadTask t = new InitialLoadTask(obj, transactionid, null);
				tasks.add(t);
			}
			beginInitialLoadTransaction(transactionid, schemaname, instance.getInstanceNumber());
			List<Future<Long>> futures = Collections.synchronizedList(new LinkedList<>());
			ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newWorkStealingPool(10);
			for (InitialLoadTask t : tasks) {
				futures.add(executor.submit(t));
			}
			executor.shutdown(); // Mark the thread pool so that it does not take additional work
			long rowcount = 0L;
			/*
			 * Every 5 seconds check which partitions have been loaded and if there was an error.
			 */
			while (futures.size() > 0) {
				Iterator<Future<Long>> iter = futures.iterator();
				while (iter.hasNext()) {
					Future<Long> f = iter.next();
					if (f.isDone()) {
						try {
							rowcount += f.get();
							futures.remove(f);
						} catch (ExecutionException | InterruptedException e) {
							/*
							 * If one partition failed, stop all other initial load threads asap
							 */
							executor.shutdownNow();
							abortTransaction();
							throw new PipelineRuntimeException("One partition failed to initial load, "
									+ "shutting down the entire initial load", e.getCause(), null);
						}
					}
				}
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					executor.shutdownNow();
					abortTransaction();
					throw new PipelineRuntimeException("Main thread got interrupted, "
							+ "shutting down the entire initial load", e.getCause(), null);
				}
			}
			commitInitialLoadTransaction();
			addOperationLogLine("Initial load for " + schemaname + "finished, loaded " + String.valueOf(rowcount) + " rows");
			logger.info("Initial load for mapping \"{}\" ... completed, loaded {} rows", schemaname, rowcount);
			return rowcount;
		} catch (SQLException e) {
			throw new PipelineRuntimeException("Failed to read the partition information", e, null);
		}
	}
	
	public class InitialLoadTask implements Callable<Long> {
	    private Integer partition;
		private String transactionid;
		private S4HanaTableMapping obj;

	    public InitialLoadTask(S4HanaTableMapping obj, String transactionid, Integer partition) {
	        this.partition = partition;
	        this.obj = obj;
	    }
	 
		public Integer getPartition() {
	        return partition;
	    }
	 
		@Override
		public Long call() throws Exception {
			S4HanaConnectionProperties props = (S4HanaConnectionProperties) instance.getConnectionProperties();
	    	try (Connection conn = S4HanaConnectorFactory.getDatabaseConnection(props);) {
				String schemaname = obj.getName();
				addOperationLogLine("Initial load for " + schemaname + ", " + getPartitionText() + " started");
				logger.info("Initial load for mapping \"{}\", {}...", schemaname, getPartitionText());
				String sql = obj.getInitialSelect(partition);
				SchemaHandler schemahandler = getSchema(obj.getName());
				Schema schema = null;
				try (PreparedStatement stmt = conn.prepareStatement(sql); ) {
					long rowcount = 0L;
					schema = obj.getAvroSchema();
					beginInitialLoadTransaction(transactionid, schemaname, instance.getInstanceNumber());
					try (ResultSet rs = stmt.executeQuery();) {
						while (rs.next()) {
							if (Thread.interrupted()) {
								throw new ConnectorRuntimeException(
										"Executing the initial load got interrupted for " + schemaname 
										+ ", " + getPartitionText(),
										null, null, null);
							}
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
					addOperationLogLine("Initial load for " + schemaname + ", " + getPartitionText() + " finished, loaded " + String.valueOf(rowcount) + " rows");
					logger.info("Initial load for mapping \"{}\", {} ... completed, loaded {} rows", schemaname, getPartitionText(), rowcount);
					return rowcount;
				} catch (SQLException e) {
					throw new ConnectorRuntimeException("Executing the initial load SQL failed with SQL error", e, 
							"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
				} catch (SchemaException e) {
					throw new ConnectorRuntimeException("SchemaException thrown when assigning the values", e, 
							null, schema.toString());
				}
	    	}
	    }

		private String getPartitionText() {
			if (partition == null) {
				return "complete table";
			} else {
				return "partition number " + String.valueOf(partition);
			}
		}

	}
}
