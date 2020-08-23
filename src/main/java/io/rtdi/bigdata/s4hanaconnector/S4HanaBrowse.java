package io.rtdi.bigdata.s4hanaconnector;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.entity.TableEntry;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorRuntimeException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.SchemaException;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.FileNameEncoder;

public class S4HanaBrowse extends BrowsingService<S4HanaConnectionProperties> {
	
	private File bopath;
	private Connection conn;

	public S4HanaBrowse(ConnectionController controller) throws IOException {
		super(controller);
		bopath = new File(controller.getDirectory(), "BusinessObjects");
	}

	@Override
	public void open() throws IOException {
		conn = S4HanaConnectorFactory.getDatabaseConnection(getConnectionProperties());
	}

	@Override
	public void close() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
			}
			conn = null;
		}
	}

	@Override
	public List<TableEntry> getRemoteSchemaNames() throws IOException {
		if (bopath.isDirectory()) {
			File[] files = bopath.listFiles();
			List<TableEntry> ret = new ArrayList<>();
			if (files != null) {
				for (File f : files) {
					if (f.getName().endsWith(".json") && f.isFile()) {
						String name = FileNameEncoder.decodeName(f.getName());
						ret.add(new TableEntry(name.substring(0, name.length()-5))); // remove the .json ending
					}
				}
			}
			return ret;
		} else {
			return null;
		}
	}

	@Override
	public Schema getRemoteSchemaOrFail(String name) throws IOException {
		S4HanaTableMapping n1 = S4HanaTableMapping.readDefinition(null, null, name, null, bopath);
		try {
			return n1.getAvroSchema();
		} catch (SchemaException e) {
			throw new ConnectorRuntimeException("Schema cannot be parsed", e, null, null);
		}
	}

	public S4HanaTableMapping getBusinessObject(String name) throws IOException {
		S4HanaTableMapping n1 = S4HanaTableMapping.readDefinition(null, null, name, null, bopath);
		return n1;
	}

	public File getBusinessObjectDirectory() {
		return bopath;
	}
	
	public List<TableImport> getHanaTables() throws ConnectorRuntimeException {
		String sql = "select t.tabname, c.ddtext from \"" + getConnectionProperties().getSourceSchema() + "\".DD02L t "
				+ "left outer join \"" + getConnectionProperties().getSourceSchema() + "\".DD02T c on (c.tabname = t.tabname and c.ddlanguage = 'E')"
				+ "where t.tabclass = 'TRANSP' "
				+ "order by 1";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			ResultSet rs = stmt.executeQuery();
			List<TableImport> sortedlist = new ArrayList<>();
			while (rs.next()) {
				String name = rs.getString(1);
				sortedlist.add(new TableImport(name));
			}
			return sortedlist;
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading all tables of the ABAP schema failed", e, 
					"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
		}
	}
	
	public Connection getConnection() {
		return conn;
	}
	
	public static class TableImport {
		private String tablename;
		private String schemaname;
		private boolean imported;
		
		public TableImport() {
			super();
		}

		public TableImport(String tablename) {
			super();
			this.tablename = tablename;
			this.schemaname = tablename;
		}

		public String getTablename() {
			return tablename;
		}
		public void setTablename(String tablename) {
			this.tablename = tablename;
		}
		public boolean isImported() {
			return imported;
		}
		public void setImported(boolean imported) {
			this.imported = imported;
		}

		public String getSchemaname() {
			return schemaname;
		}

		public void setSchemaname(String schemaname) {
			this.schemaname = schemaname;
		}
	}

	@Override
	public void validate() throws IOException {
		close();
		open();
		String sql = "select top 1 t.tabname, c.ddtext from \"" + getConnectionProperties().getSourceSchema() + "\".DD02L t "
				+ "left outer join \"" + getConnectionProperties().getSourceSchema() + "\".DD02T c on (c.tabname = t.tabname and c.ddlanguage = 'E')"
				+ "where t.tabclass = 'TRANSP' "
				+ "order by 1";
		try (PreparedStatement stmt = conn.prepareStatement(sql);) {
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return;
			} else {
				throw new ConnectorRuntimeException("reading the ABAP data dictionary did work but not return any data", null, 
						"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
			}
		} catch (SQLException e) {
			throw new ConnectorRuntimeException("Reading all tables of the ABAP schema failed", e, 
					"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
		} finally {
			close();
		}
		
	}

	@Override
	public void deleteRemoteSchemaOrFail(String remotename) throws IOException {
		File file = new File(bopath, remotename + ".json");
		java.nio.file.Files.delete(file.toPath());
	}
}
