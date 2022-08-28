package io.rtdi.bigdata.s4hanaconnector;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.FileNameEncoder;
import io.rtdi.bigdata.kafka.avro.SchemaConstants;
import io.rtdi.bigdata.kafka.avro.datatypes.*;
import io.rtdi.bigdata.kafka.avro.recordbuilders.AvroField;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

public class S4HanaTableMapping {
	private String mastertable; // e.g. salesorder as L1
	private String alias;
	private List<ColumnMapping> columnmappings; // e.g. orderid <- L1.orderid  
	private List<String> pkcolumns;
	private static ObjectMapper mapper = new ObjectMapper();
	protected Schema avroschema = null;

	private Connection conn;
	private String sourcedbschema;
	private String username;
	private String name;
	private String deltaselect;
	private StringBuffer initialselectprojection;
	private String initialloadwhere;

	public S4HanaTableMapping() {
		super();
	}

	public S4HanaTableMapping(String name, String username, String sourcedbschema, String mastertable, String alias, String initialloadwhere, Connection conn) throws ConnectorRuntimeException {
		super();
		this.mastertable = mastertable;
		this.alias = alias;
		this.sourcedbschema = sourcedbschema;
		this.username = username;
		this.conn = conn;
		this.name = name;
		this.initialloadwhere = initialloadwhere;
		addColumns();
	}

	private S4HanaTableMapping(String username, String sourcedbschema, String name, Connection conn) throws ConnectorRuntimeException {
		super();
		this.sourcedbschema = sourcedbschema;
		this.username = username;
		this.conn = conn;
		this.name = name;
	}

	public void read(File directory) throws PropertiesException {
		if (!directory.exists()) {
			throw new PropertiesException("Directory for the Relational Object Definition files does not exist", "Use the UI or create the file manually", directory.getAbsolutePath());
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists but is no directory", (String) null, directory.getAbsolutePath());
		} else { 
			File file = new File(directory, FileNameEncoder.encodeName(name + ".json"));
			if (!file.canRead()) {
				throw new PropertiesException("Properties file is not read-able", "Check file permissions and users", file.getAbsolutePath());
			} else {
				try {
					S4HanaTableMapping data = mapper.readValue(file, this.getClass());
				    parseValues(data);
				} catch (PropertiesException e) {
					throw e; // to avoid nesting the exception
				} catch (IOException e) {
					throw new PropertiesException("Cannot parse the json file with the properties", e, "check filename and format", file.getName());
				}
			}
		}
	}

	public void write(File directory) throws PropertiesException {
		if (!directory.exists()) {
			// throw new PropertiesException("Directory for the Relational Object Definition files does not exist", "Use the UI or create the file manually", 10005, directory.getAbsolutePath());
			directory.mkdirs();
		}
		if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists but is no directory", (String) null, directory.getAbsolutePath());
		} else {
			File file = new File(directory, FileNameEncoder.encodeName(name + ".json"));
			if (file.exists() && !file.canWrite()) { // Either the file does not exist or it exists and is write-able
				throw new PropertiesException("Properties file is not write-able", "Check file permissions and users", file.getAbsolutePath());
			} else {
				/*
				 * When writing the properties to a file, all the extra elements like description etc should not be stored.
				 * Therefore a simplified version of the property tree needs to be created.
				 */
				try {
					mapper.setSerializationInclusion(Include.NON_NULL);
					mapper.writeValue(file, this);
				} catch (IOException e) {
					throw new PropertiesException("Failed to write the json Relational Object Definition file", e, "check filename", file.getName());
				}
				
			}
		}
	}

	void createTrigger() throws ConnectorRuntimeException {
		String sql = "select right(trigger_name, 1) from triggers " + 
				"where subject_table_schema = ? and subject_table_name = ? and trigger_name like subject_table_name || '\\_t\\__' escape '\\'";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, sourcedbschema);
			stmt.setString(2, getMastertable());
			ResultSet rs = stmt.executeQuery();
			Set<String> existingtriggers = new HashSet<>();
			while (rs.next()) {
				existingtriggers.add(rs.getString(1));
			}
			if (existingtriggers.size() < 3) {
				if (getPKColumns().size() == 0) {
					throw new ConnectorRuntimeException("This replication technology does only work on tables with primary keys", null, 
							"Please remove the table specified from the list of tables to be replicated", getMastertable());
				} else if (getPKColumns().size() > 6) {
					throw new ConnectorRuntimeException("Currently only tables with up the six primary keys are allowed", null, 
							"Please create and issue", getMastertable());
				} else {
					String sourceidentifier = "\"" + sourcedbschema + "\".\"" + getMastertable() + "\"";
					StringBuffer pklist1 = new StringBuffer();
					StringBuffer pklist2 = new StringBuffer();
					StringBuffer pklist3 = new StringBuffer();
					StringBuffer pklistdifferent = new StringBuffer();
					for (int i = 0; i < getPKColumns().size(); i++) {
						String pkcolumn = getPKColumns().get(i);
						if (pkcolumn == null) {
							throw new ConnectorRuntimeException("The table is not using all primary key columns", null, 
									"Make sure all pk columns are mapped at least", getMastertable() + ": " + getPKColumns().toString());
						}
						if (i != 0) {
							pklist1.append(',');
							pklist2.append(',');
							pklist3.append(',');
							pklistdifferent.append(" OR ");
						}
						// :c."MANDT",:c."VBELN"
						pklist1.append(":c.\"");
						pklist1.append(pkcolumn);
						pklist1.append('"');
						// PK1,PK2
						pklist2.append("PK");
						pklist2.append(i+1);
						// :o."MANDT",:o."VBELN"
						pklist3.append(":c.\"");
						pklist3.append(pkcolumn);
						pklist3.append('"');
						// :o."MANDT" <> :c."MANDT" OR :o."VBELN" <> :c."VBELN"
						pklistdifferent.append(":o.\"");
						pklistdifferent.append(pkcolumn);
						pklistdifferent.append("\" <> :c.\"");
						pklistdifferent.append(pkcolumn);
						pklistdifferent.append('"');
					}
					if (!existingtriggers.contains("i")) {
						sql = "CREATE TRIGGER \"" + getMastertable() + "_t_i\" \r\n" + 
								" AFTER INSERT ON " + sourceidentifier + " \r\n" + 
								" REFERENCING NEW ROW c \r\n" + 
								" FOR EACH ROW \r\n" + 
								" BEGIN \r\n" + 
								"     INSERT INTO \"" + username + "\".PKLOG \r\n" +
								"       (change_ts, schema_name, table_name, change_type, \r\n" +
								"       transactionid, transaction_seq, \r\n"  +
								"      " + pklist2.toString() + ") \r\n" + 
								"     VALUES (now(), '" + sourcedbschema + "', '" + getMastertable() + "', 'I', \r\n" +
								"       CURRENT_UPDATE_TRANSACTION(), CURRENT_UPDATE_STATEMENT_SEQUENCE(), \r\n" +
								"       " + pklist1.toString() + " ); \r\n" + 
								" END"; 
						try (PreparedStatement stmttr = conn.prepareStatement(sql);) {
							stmttr.execute();
						}
					}
					if (!existingtriggers.contains("u")) {
						sql =   "CREATE TRIGGER \"" + getMastertable() + "_t_u\" \r\n" + 
								" AFTER UPDATE ON " + sourceidentifier + " \r\n" + 
								" REFERENCING NEW ROW c, OLD ROW o \r\n" + 
								" FOR EACH ROW \r\n" + 
								" BEGIN \r\n" + 
								"     INSERT INTO \"" + username + "\".PKLOG \r\n" +
								"       (change_ts, schema_name, table_name, change_type, \r\n" +
								"       transactionid, transaction_seq, \r\n" +
								"      " + pklist2.toString() + ") \r\n" + 
								"     VALUES (now(), '" + sourcedbschema + "', '" + getMastertable() + "', 'U', \r\n" +
								"       CURRENT_UPDATE_TRANSACTION(), CURRENT_UPDATE_STATEMENT_SEQUENCE(), \r\n" +
								"       " + pklist1.toString() + " ); \r\n" + 
								"     IF (" + pklistdifferent.toString() + " ) THEN \r\n" + 
								"       INSERT INTO \"" + username + "\".PKLOG \r\n" +
								"         (change_ts, schema_name, table_name, change_type, \r\n" +
								"         transactionid, transaction_seq, \r\n" +
								"        " + pklist2.toString() + ") \r\n" + 
								"       VALUES (now(), '" + sourcedbschema + "', '" + getMastertable() + "', 'U', \r\n" +
								"         CURRENT_UPDATE_TRANSACTION(), CURRENT_UPDATE_STATEMENT_SEQUENCE(), \r\n" +
								"         " + pklist3.toString() + " ); \r\n" + 
								"     END IF; \r\n" +
								"END"; 
						try (PreparedStatement stmttr = conn.prepareStatement(sql);) {
							stmttr.execute();
						}
					}
					if (!existingtriggers.contains("d")) {
						sql =   "CREATE TRIGGER \"" + getMastertable() + "_t_d\" \r\n" + 
								" AFTER DELETE ON " + sourceidentifier + " \r\n" + 
								" REFERENCING OLD ROW c \r\n" + 
								" FOR EACH ROW \r\n" + 
								" BEGIN \r\n" + 
								"     INSERT INTO \"" + username + "\".PKLOG \r\n" +
								"      (change_ts, schema_name, table_name, change_type, \r\n" +
								"      transactionid, transaction_seq, \r\n" +
								"      " + pklist2.toString() + ") \r\n" + 
								"     VALUES (now(), '" + sourcedbschema + "', '" + getMastertable() + "', 'D', \r\n" +
								"       CURRENT_UPDATE_TRANSACTION(), CURRENT_UPDATE_STATEMENT_SEQUENCE(), \r\n" +
								"       " + pklist1.toString() + " ); \r\n" + 
								"END"; 
						try (PreparedStatement stmttr = conn.prepareStatement(sql);) {
							stmttr.execute();
						}
					}
				}

			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Creating the Change Logging triggers failed in the database", e, 
					"Execute the sql as Hana user \"" + username + "\"", sql);
		}
	}

	protected void parseValues(S4HanaTableMapping data) throws ConnectorRuntimeException {
		this.mastertable = data.getMastertable();
		this.columnmappings = data.getColumnmappings();
		this.pkcolumns = data.getPKColumns();
		this.alias = data.getAlias();
		this.initialloadwhere = data.getInitialloadwhere();
	}

	public void setMastertable(String mastertable) {
		this.mastertable = mastertable;
	}

	public String getAlias() {
		return alias;
	}
	
	private String getAliasIdentifier() {
		return "\"" + alias + "\"";
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public void addColumns() throws ConnectorRuntimeException {
		String sql = "select c.column_name, c.data_type_name, c.length, c.scale, p.position \r\n" + 
				"from table_columns c left outer join constraints p \r\n" + 
				"	on (p.is_primary_key = 'TRUE' and p.schema_name = c.schema_name and p.table_name = c.table_name and p.column_name = c.column_name) \r\n" + 
				"where c.schema_name = ? and c.table_name = ? \r\n" + 
				"order by c.position";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, sourcedbschema);
			stmt.setString(2, mastertable);
			ResultSet rs = stmt.executeQuery();
			int columncount = 0;
			while (rs.next()) {
				ColumnMapping m = addMapping(rs.getString(1), "\"" + alias + "\".\"" + rs.getString(1) + "\"", getHanaDataType(rs.getString(2), rs.getInt(3), rs.getInt(4)));
				if (rs.getInt(5) != 0) {
					addPK(rs.getInt(5), m);
				}
				columncount++;
			}
			if (columncount == 0) {
				throw new ConnectorRuntimeException("This table does not seem to exist in the Hana database itself - not activated?, not a transparent table?", null, 
						"Execute the sql as Hana user \"" + username + "\"", sql);
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading the table definition failed", e, 
					"Execute the sql as Hana user \"" + username + "\"", sql);
		}
		if (pkcolumns == null || pkcolumns.size() == 0) {
			buildPKviaDDIC();
		}
	}

	/**
	 * Some SAP table do have a PK defined in ABAP but not enforced via a primary key constraint in the database, ACDOCA is a common example.
	 * Therefore, if no PK is found in the database, read it from DD03L.
	 * @throws ConnectorRuntimeException
	 */
	private void buildPKviaDDIC() throws ConnectorRuntimeException {
		String sql = "select fieldname from \"" + sourcedbschema + "\".DD03L where keyflag = 'X' and as4local = 'A' and tabname = ? order by position;";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, mastertable);
			ResultSet rs = stmt.executeQuery();
			pkcolumns = new ArrayList<>();
			while (rs.next()) {
				pkcolumns.add(rs.getString(1));
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading the table definition failed", e, 
					"Execute the sql as Hana user \"" + username + "\"", sql);
		}
	}

	public void addPK(int pos, ColumnMapping m) {
		if (pkcolumns == null) {
			pkcolumns = new ArrayList<>();
		}
		if (pkcolumns.size() < pos) {
			while (pkcolumns.size() < pos-1) {
				pkcolumns.add(null);
			}
			pkcolumns.add(m.getAlias());
		} else {
			pkcolumns.set(pos, m.getAlias());
		}
	}

	private static String getHanaDataType(String datatype, int length, int scale) {
		switch (datatype) {
		case "DECIMAL": // decimal(p, s) with p between 1..38 and scale 0..p
			return datatype + "(" + length + ", " + scale + ")";
		case "CHAR":
		case "VARCHAR":
		case "VARBINARY":
		case "NCHAR":
		case "NVARCHAR":
			return datatype + "(" + length + ")";
		default:
			return datatype;
		}
	}

	public ColumnMapping addMapping(String columnname, String sqlexpression, String hanadatatype) {
		if (columnmappings == null) {
			columnmappings = new ArrayList<>();
		}
		ColumnMapping m = new ColumnMapping(columnname, sqlexpression, hanadatatype);
		columnmappings.add(m);
		return m;
	}

	public static S4HanaTableMapping readDefinition(String username, String sourcedbschema, String name, Connection conn, File directory) throws PropertiesException {
		S4HanaTableMapping o = new S4HanaTableMapping(username, sourcedbschema, name, conn);
		o.read(directory);
		return o;
	}
	
	@JsonIgnore
	public void setConnection(S4HanaBrowse browser) {
		this.sourcedbschema = browser.getConnectionProperties().getSourceSchema();
		this.username = browser.getConnectionProperties().getUsername();
		this.conn = browser.getConnection();
	}

	@JsonIgnore
	protected Connection getConn() {
		return conn;
	}

	@JsonIgnore
	protected String getSourcedbschema() {
		return sourcedbschema;
	}

	@JsonIgnore
	protected String getUsername() {
		return username;
	}

	
	@JsonIgnore
	public Schema getAvroSchema() throws SchemaException, ConnectorRuntimeException {
		if (avroschema == null) {
			ValueSchema v = new ValueSchema(getName(), null);
			createSchema(v);
			v.build();
		}
		return avroschema;
	}
	
	public void createDeltaObjects() throws ConnectorRuntimeException, SQLException {
		createTrigger();
		createView();
		deltaselect = createSelectDelta().toString();
		initialselectprojection = createProjectionInitial(this);
	}

	private void createView() throws ConnectorRuntimeException {
		try {
			String sql = "drop view \"" + getMastertable() + "_CHANGE_VIEW\" cascade";
			CallableStatement callable = conn.prepareCall(sql);
			callable.execute();
		} catch (SQLException e) {
			// ignore views that do not exist
		}
		StringBuffer sql = new StringBuffer();
		sql.append("create view \"");
		sql.append(getMastertable());
		sql.append("_CHANGE_VIEW\" as \r\n");
		addChangeSQL(sql);
		try (CallableStatement callable = conn.prepareCall(sql.toString());) {
			callable.execute();
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Creating the change view failed", e, 
					"Execute the sql as Hana user \"" + username + "\"", sql.toString());
		}
	}
	
	protected void addChangeSQL(StringBuffer sql) {
		sql.append("select ");
		for (int i = 0; i < getPKColumns().size(); i++) {
			sql.append("PK");
			sql.append(i+1);
			sql.append(" as \"");
			sql.append(getPKColumns().get(i));
			sql.append("\", ");
		}
		sql.append("transactionid as _transactionid from pklog where table_name = '");
		sql.append(getMastertable());
		sql.append("' and schema_name = '");
		sql.append(sourcedbschema);
		sql.append("'\r\n");
	}

	private StringBuffer createSelectDelta() {
		StringBuffer conditions = createRootJoinCondition(this);
		StringBuffer select = new StringBuffer();
		select.append("select ");
		select.append("case when ").append(getAliasIdentifier()).append(".\"");
		select.append(getPKColumns().get(0));
		select.append("\" is null then 'D' else 'A' end as _change_type, \r\n");
		select.append("l._transactionid as _transactionid,\r\n");
		select.append(getAliasIdentifier()).append(".\"$rowid$\" as \"").append(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID).append("\", \r\n");
		select.append(createProjectionDelta(this, true));
		select.append("\r\nfrom (select max(_transactionid) as _transactionid, ");
		select.append(getPKList());
		select.append(" from \"");
		select.append(getMastertable());
		select.append("_CHANGE_VIEW\" where _transactionid > ? and _transactionid <= ?\r\n");
		select.append("group by ");
		select.append(getPKList());
		select.append(") l \r\n");
		select.append("left outer join \"");
		select.append(sourcedbschema);
		select.append("\".\"");
		select.append(getMastertable());
		select.append("\" as ").append(getAliasIdentifier()).append("\r\n");
		select.append("on (");
		select.append(conditions);
		select.append(");\r\n");
		return select;
	}

	public String getMastertable() {
		return mastertable;
	}

	protected String getPKList() {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < getPKColumns().size(); i++) {
			String columnname = getPKColumns().get(i);
			if (i != 0) {
				b.append(", ");
			}
			b.append('"');
			b.append(columnname);
			b.append('"');
		}
		return b.toString();
	}

	public static boolean checktable(String tablename, Connection conn) throws ConnectorRuntimeException {
		String sql = "select 1 from tables where table_name = ? and schema_name = current_user";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, tablename);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return true;
			} else {
				return false;
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Checking if the table exists failed in the database", e, 
					"Execute the sql as Hana user", sql);
		}
	}

	private static StringBuffer createRootJoinCondition(S4HanaTableMapping r) {
		StringBuffer conditions = new StringBuffer();
		for (int i = 0; i < r.getPKColumns().size(); i++) {
			String columnname = r.getPKColumns().get(i);
			if (i != 0) {
				conditions.append(" and ");
			}
			conditions.append("l.\"");
			conditions.append(columnname);
			conditions.append("\" = ").append(r.getAliasIdentifier()).append(".\"");
			conditions.append(columnname);
			conditions.append("\"");
		}
		return conditions;
	}

	private static StringBuffer createProjectionDelta(S4HanaTableMapping r, boolean usedriver) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < r.getColumnmappings().size(); i++) {
			ColumnMapping column = r.getColumnmappings().get(i);
			if (i != 0) {
				b.append(", ");
			}
			if (usedriver && r.getPKColumns().contains(column.getTableColumnName())) {
				b.append("l.\"");
				b.append(column.getTableColumnName());
				b.append("\" as \"");
				b.append(column.getAlias());
				b.append("\"");
			} else {
				b.append(column.getSql());
				b.append(" as \"");
				b.append(column.getAlias());
				b.append("\"");
			}
		}
		return b;
	}

	private static StringBuffer createProjectionInitial(S4HanaTableMapping r) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < r.getColumnmappings().size(); i++) {
			ColumnMapping column = r.getColumnmappings().get(i);
			if (i != 0) {
				b.append(", ");
			}
			b.append(column.getSql());
			b.append(" as \"");
			b.append(column.getAlias());
			b.append("\"");
		}
		return b;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<ColumnMapping> getColumnmappings() {
		return columnmappings;
	}

	public void setColumnmappings(List<ColumnMapping> columnmappings) {
		this.columnmappings = columnmappings;
	}

	protected void createSchema(SchemaBuilder valueschema) throws ConnectorRuntimeException {
		try {
			if (getColumnmappings() != null) {
				for ( ColumnMapping m : getColumnmappings()) {
					String hanadatatypestring = m.getHanadatatype();
					String columnname = m.getAlias();
					AvroField f = valueschema.add(columnname, getDataType(hanadatatypestring), null, true);
					if (getPKColumns().contains(m.getTableColumnName())) {
						f.setPrimaryKey();
					}
				}
				avroschema = valueschema.getSchema();
			} else {
				throw new ConnectorRuntimeException("The schema definition file does not contain any columns!", null, 
						"Something was wrong when the schema mapping file got created", this.getName());
			}
		} catch (SchemaException e) {
			throw new ConnectorRuntimeException("The Avro Schema cannot be created due to an internal error", e, 
					"Please create an issue", valueschema.toString());
		}
	}

	public List<String> getPKColumns() {
		return pkcolumns;
	}

	public static Schema getDataType(String datatypestring) throws ConnectorRuntimeException {
		Pattern p = Pattern.compile("(\\w*)\\s*\\(?\\s*(\\d*)\\s*\\,?\\s*(\\d*)\\s*\\)?.*");  // decimal(10,3) plus spaces anywhere, three groups
		Matcher m = p.matcher(datatypestring);
		m.matches();
		String datatype = m.group(1);
		String lengthstring = m.group(2);
		int length = 0;
		int scale = 0;
		if (lengthstring != null && lengthstring.length() != 0) {
			length = Integer.valueOf(lengthstring);
			String scalestring = m.group(3);
			if (scalestring != null && scalestring.length() != 0) {
				scale = Integer.valueOf(scalestring);
			}
		}
		switch (datatype) {
		case "TINYINT": // 8-bit unsigned(!!) integer; 0..255
			return AvroShort.getSchema();
		case "SMALLINT": // 16-bit signed integer; -32,768..32,767
			return AvroShort.getSchema();
		case "INTEGER": // 32-bit signed integer; -2,147,483,648..2,147,483,647
			return AvroInt.getSchema();
		case "BIGINT": // 64-bit signed integer
			return AvroLong.getSchema();
		case "DECIMAL": // decimal(p, s) with p between 1..38 and scale 0..p
			return AvroDecimal.getSchema(length, scale);
		case "REAL": // 32-bit floating-point number
			return AvroFloat.getSchema();
		case "DOUBLE": // 64-bit floating-point number
			return AvroDouble.getSchema();
		case "SMALLDECIMAL": // The precision and scale can vary within the range 1~16 for precision and -369~368 for scale
			return AvroDecimal.getSchema(length, scale);
		case "CHAR":
		case "VARCHAR": // 7-bit ASCII string
			return AvroVarchar.getSchema(length);
		case "BINARY":
			return AvroBytes.getSchema();
		case "VARBINARY":
			return AvroBytes.getSchema();
		case "DATE":
			return AvroDate.getSchema();
		case "TIME":
			return AvroTime.getSchema();
		case "TIMESTAMP":
			return AvroTimestampMicros.getSchema();
		case "CLOB":
			return AvroCLOB.getSchema();
		case "BLOB":
			return AvroBytes.getSchema();
		case "NCHAR":
			return AvroNVarchar.getSchema(length);
		case "NVARCHAR":
			return AvroNVarchar.getSchema(length);
		case "ALPHANUM":
			return AvroVarchar.getSchema(length);
		case "NCLOB":
			return AvroNCLOB.getSchema();
		case "TEXT":
			return AvroNCLOB.getSchema();
		case "BINTEXT":
			return AvroBytes.getSchema();
		case "SHORTTEXT":
			return AvroNCLOB.getSchema();
		case "SECONDDATE":
			return AvroTimestamp.getSchema();
		case "ST_POINT":
			return AvroSTPoint.getSchema();
		case "ST_GEOMETRY":
			return AvroSTGeometry.getSchema();
		case "BOOLEAN":
			return AvroBoolean.getSchema();
		default:
			throw new ConnectorRuntimeException("Table contains a data type which is not known", null, "Newer Hana version??", datatype);
		}
	}

	public String getDeltaSelect() {
		return deltaselect;
	}
	
	public static class ColumnMapping {
		private String alias;
		private String sql;
		private String hanadatatype;
		private String tablecolumnname;

		public ColumnMapping() {
		}
		
		public ColumnMapping(String alias, String sqlexpression, String hanadatatype) {
			this.alias = alias;
			this.hanadatatype = hanadatatype;
			setSql(sqlexpression);
		}
		public String getAlias() {
			return alias;
		}
		
		public void setAlias(String alias) {
			this.alias = alias;
		}
		
		public String getSql() {
			return sql;
		}
		
		public void setSql(String sql) {
			this.sql = sql;
			String[] comp = sql.split("\\.");
			if (comp.length == 2) {
				tablecolumnname = comp[1];
			} else {
				tablecolumnname = comp[0];
			}
			if (tablecolumnname.charAt(0) == '"') {
				tablecolumnname = tablecolumnname.substring(1, tablecolumnname.length()-1);
			}
		}

		public String getHanadatatype() {
			return hanadatatype;
		}

		public void setHanadatatype(String hanadatatype) {
			this.hanadatatype = hanadatatype;
		}

		@Override
		public String toString() {
			return alias;
		}

		protected String getTableColumnName() {
			return tablecolumnname;
		}
	}

	public String getInitialSelect(Integer partition) {
		StringBuffer select = new StringBuffer();
		select.append("select 'I' as _change_type, \r\n");
		select.append("null as _transactionid, \r\n");
		select.append(getAliasIdentifier()).append(".\"$rowid$\" as \"").append(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID).append("\", \r\n");
		select.append(initialselectprojection);
		select.append("\r\n from \"");
		select.append(sourcedbschema);
		select.append("\".\"");
		select.append(getMastertable());
		select.append("\" ");
		if (partition != null) {
			select.append(" partition (");
			select.append(partition);
			select.append(") ");
		}
		select.append(" as ").append(getAliasIdentifier());
		if (initialloadwhere != null) {
			select.append(" where ").append(initialloadwhere);
		}
		return select.toString();
	}

	@Override
	public String toString() {
		return mastertable;
	}

	public String getInitialloadwhere() {
		return initialloadwhere;
	}

	public void setInitialloadwhere(String initialloadwhere) {
		this.initialloadwhere = initialloadwhere;
	}


}
