package io.rtdi.bigdata.s4hanaconnector;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroBoolean;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroBytes;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroCLOB;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDate;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDecimal;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDouble;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroFloat;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroInt;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroLong;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroNCLOB;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroNVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroSTGeometry;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroSTPoint;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroShort;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTime;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestampMicros;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.ValueSchema;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;

public class HanaBusinessObject {
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
	private String initialselect;

	public HanaBusinessObject() {
		super();
	}

	public HanaBusinessObject(String name, String username, String sourcedbschema, String mastertable, String alias, Connection conn) throws ConnectorRuntimeException {
		super();
		this.mastertable = mastertable;
		this.alias = alias;
		this.sourcedbschema = sourcedbschema;
		this.username = username;
		this.conn = conn;
		this.name = name;
		addColumns();
	}

	private HanaBusinessObject(String username, String sourcedbschema, String name, Connection conn) throws ConnectorRuntimeException {
		super();
		this.sourcedbschema = sourcedbschema;
		this.username = username;
		this.conn = conn;
		this.name = name;
	}

	public void read(File directory, String name) throws PropertiesException {
		if (!directory.exists()) {
			throw new PropertiesException("Directory for the Relational Object Definition files does not exist", "Use the UI or create the file manually", 10005, directory.getAbsolutePath());
		} else if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists but is no directory", null, 10005, directory.getAbsolutePath());
		} else { 
			File file = new File(directory.getAbsolutePath() + File.separatorChar + IOUtils.encodeFileName(name) + ".json");
			if (!file.canRead()) {
				throw new PropertiesException("Properties file is not read-able", "Check file permissions and users", 10005, file.getAbsolutePath());
			} else {
				try {
					HanaBusinessObject data = mapper.readValue(file, this.getClass());
				    parseValues(data);
				} catch (PropertiesException e) {
					throw e; // to avoid nesting the exception
				} catch (IOException e) {
					throw new PropertiesException("Cannot parse the json file with the properties", e, "check filename and format", file.getName());
				}
			}
		}
	}

	public void write(File directory, String name) throws PropertiesException {
		if (!directory.exists()) {
			// throw new PropertiesException("Directory for the Relational Object Definition files does not exist", "Use the UI or create the file manually", 10005, directory.getAbsolutePath());
			directory.mkdirs();
		}
		if (!directory.isDirectory()) {
			throw new PropertiesException("Specified location exists but is no directory", null, 10005, directory.getAbsolutePath());
		} else {
			File file = new File(directory.getAbsolutePath() + File.separatorChar + IOUtils.encodeFileName(name) + ".json");
			if (file.exists() && !file.canWrite()) { // Either the file does not exist or it exists and is write-able
				throw new PropertiesException("Properties file is not write-able", "Check file permissions and users", 10005, file.getAbsolutePath());
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
			String existingtriggers = "";
			while (rs.next()) {
				existingtriggers += rs.getString(1);
			}
			if (existingtriggers.length() < 3) {
				StringBuffer pklist1 = new StringBuffer();
				StringBuffer pklist2 = new StringBuffer();
				for (int i = 0; i < getPKColumns().size(); i++) {
					String pkcolumnname = getPKColumns().get(i);
					if (pkcolumnname == null) {
						throw new ConnectorRuntimeException("The table is not using all primary key columns", null, 
								"Make sure all pk columns are mapped at least", getMastertable() + ": " + getPKColumns().toString());
					}
					if (i != 0) {
						pklist1.append(',');
						pklist2.append(',');
					}
					pklist1.append(":c.\"");
					pklist1.append(pkcolumnname);
					pklist1.append('"');
					pklist2.append("PK");
					pklist2.append(i+1);
				}
				if (getPKColumns().size() == 0) {
					throw new ConnectorRuntimeException("This replication technology does only work on tables with primary keys", null, 
							"Please remove the table specified from the list of tables to be replicated", getMastertable());
				} else if (getPKColumns().size() > 6) {
					throw new ConnectorRuntimeException("Currently only tables with up the six primary keys are allowed", null, 
							"Please create and issue", getMastertable() + ": " + pklist1.toString());
				} else {
					if (!existingtriggers.contains("i")) {
						sql = "CREATE TRIGGER \"" + getMastertable() + "_t_i\" " + 
								" AFTER INSERT ON \"" + sourcedbschema + "\".\"" + getMastertable() + "\" " + 
								" REFERENCING NEW ROW c " + 
								" FOR EACH ROW " + 
								" BEGIN " + 
								"     INSERT INTO \"" + username + "\".PKLOG "
										+ "(change_ts, schema_name, table_name, change_type, "
										+ "transactionid, transaction_seq, "
										+ pklist2.toString() + ") " + 
								"     VALUES (now(), '" + sourcedbschema + "', '" + getMastertable() + "', 'I', "
										+ "CURRENT_UPDATE_TRANSACTION(), CURRENT_UPDATE_STATEMENT_SEQUENCE(), "
										+ pklist1.toString() + " ); " + 
								" END"; 
						try (PreparedStatement stmttr = conn.prepareStatement(sql);) {
							stmttr.execute();
						}
					}
					if (!existingtriggers.contains("u")) {
						sql = "CREATE TRIGGER \"" + getMastertable() + "_t_u\" " + 
								" AFTER UPDATE ON \"" + sourcedbschema + "\".\"" + getMastertable() + "\" " + 
								" REFERENCING NEW ROW c " + 
								" FOR EACH ROW " + 
								" BEGIN " + 
								"     INSERT INTO \"" + username + "\".PKLOG "
										+ "(change_ts, schema_name, table_name, change_type, "
										+ "transactionid, transaction_seq, "
										+ pklist2.toString() + ") " + 
								"     VALUES (now(), '" + sourcedbschema + "', '" + getMastertable() + "', 'U', "
										+ "CURRENT_UPDATE_TRANSACTION(), CURRENT_UPDATE_STATEMENT_SEQUENCE(), "
										+ pklist1.toString() + " ); " + 
								" END"; 
						try (PreparedStatement stmttr = conn.prepareStatement(sql);) {
							stmttr.execute();
						}
					}
					if (!existingtriggers.contains("d")) {
						sql = "CREATE TRIGGER \"" + getMastertable() + "_t_d\" " + 
								" BEFORE DELETE ON \"" + sourcedbschema + "\".\"" + getMastertable() + "\" " + 
								" REFERENCING OLD ROW c " + 
								" FOR EACH ROW " + 
								" BEGIN " + 
								"     INSERT INTO \"" + username + "\".PKLOG "
										+ "(change_ts, schema_name, table_name, change_type, "
										+ "transactionid, transaction_seq, "
										+ pklist2.toString() + ") " + 
								"     VALUES (now(), '" + sourcedbschema + "', '" + getMastertable() + "', 'D', "
										+ "CURRENT_UPDATE_TRANSACTION(), CURRENT_UPDATE_STATEMENT_SEQUENCE(), "
										+ pklist1.toString() + " ); " + 
								" END"; 
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

	protected void parseValues(HanaBusinessObject data) throws ConnectorRuntimeException {
		this.mastertable = data.getMastertable();
		this.columnmappings = data.getColumnmappings();
		this.pkcolumns = data.getPKColumns();
		this.alias = data.getAlias();
	}

	public void setMastertable(String mastertable) {
		this.mastertable = mastertable;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public void addColumns() throws ConnectorRuntimeException {
		String sql = "select c.column_name, c.data_type_name, c.length, c.scale, p.position " + 
				"from table_columns c left outer join constraints p " + 
				"	on (p.is_primary_key = 'TRUE' and p.schema_name = c.schema_name and p.table_name = c.table_name and p.column_name = c.column_name) " + 
				"where c.schema_name = ? and c.table_name = ? " + 
				"order by c.position";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			stmt.setString(1, sourcedbschema);
			stmt.setString(2, mastertable);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ColumnMapping m = addMapping(rs.getString(1), "\"" + alias + "\".\"" + rs.getString(1) + "\"", getHanaDataType(rs.getString(2), rs.getInt(3), rs.getInt(4)));
				if (rs.getInt(5) != 0) {
					addPK(rs.getInt(5), m);
				}
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

	public static HanaBusinessObject readDefinition(String username, String sourcedbschema, String name, Connection conn, File directory) throws PropertiesException {
		HanaBusinessObject o = new HanaBusinessObject(username, sourcedbschema, name, conn);
		o.read(directory, name);
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
			ValueSchema v = new ValueSchema(getMastertable(), null, null);
			createSchema(v);
			v.build();
		}
		return avroschema;
	}
	
	public void createDeltaObjects() throws ConnectorRuntimeException, SQLException {
		createTrigger();
		createView();
		deltaselect = createSelectDelta().toString();
		initialselect = createSelectInitial().toString();
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
		select.append("case when d.\"");
		select.append(getPKColumns().get(0));
		select.append("\" is null then 'D' else 'A' end as _change_type, l._transactionid as _transactionid,\r\n");
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
		select.append("\" d \r\n");
		select.append("on (");
		select.append(conditions);
		select.append(");\r\n");
		return select;
	}

	private StringBuffer createSelectInitial() {
		StringBuffer select = new StringBuffer();
		select.append("select 'I' as _change_type, null as _transactionid, ");
		select.append(createProjectionInitial(this));
		select.append("\r\n from \"");
		select.append(sourcedbschema);
		select.append("\".\"");
		select.append(getMastertable());
		select.append("\" d");
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

	private static StringBuffer createRootJoinCondition(HanaBusinessObject r) {
		StringBuffer conditions = new StringBuffer();
		for (int i = 0; i < r.getPKColumns().size(); i++) {
			String columnname = r.getPKColumns().get(i);
			if (i != 0) {
				conditions.append(" and ");
			}
			conditions.append("l.\"");
			conditions.append(columnname);
			conditions.append("\" = d.\"");
			conditions.append(columnname);
			conditions.append("\"");
		}
		return conditions;
	}

	private static StringBuffer createProjectionDelta(HanaBusinessObject r, boolean usedriver) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < r.getColumnmappings().size(); i++) {
			String columnname = r.getColumnmappings().get(i).getAlias();
			if (i != 0) {
				b.append(", ");
			}
			if (usedriver && r.getPKColumns().contains(columnname)) {
				b.append("l");
			} else {
				b.append("d");
			}
			b.append(".\"");
			b.append(columnname);
			b.append('"');
		}
		return b;
	}

	private static StringBuffer createProjectionInitial(HanaBusinessObject r) {
		StringBuffer b = new StringBuffer();
		for (int i = 0; i < r.getColumnmappings().size(); i++) {
			String columnname = r.getColumnmappings().get(i).getAlias();
			if (i != 0) {
				b.append(", ");
			}
			b.append("\"");
			b.append(columnname);
			b.append('"');
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
			for ( ColumnMapping m : getColumnmappings()) {
				String hanadatatypestring = m.getHanadatatype();
				String columnname = m.getAlias();
				AvroField f = valueschema.add(columnname, getDataType(hanadatatypestring), null, true);
				if (getPKColumns().contains(m.getAlias())) {
					f.setPrimaryKey();
				}
			}
			avroschema = valueschema.getSchema();
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

		public ColumnMapping() {
		}
		
		public ColumnMapping(String columnname, String sqlexpression, String hanadatatype) {
			this.alias = columnname;
			this.sql = sqlexpression;
			this.hanadatatype = hanadatatype;
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

	}

	public String getInitialSelect() {
		return initialselect;
	}

	@Override
	public String toString() {
		return mastertable;
	}


}
