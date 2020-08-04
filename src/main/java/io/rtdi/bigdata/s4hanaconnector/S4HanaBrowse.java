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
import io.rtdi.bigdata.connector.pipeline.foundation.utils.NameEncoder;

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
			List<TableEntry> ret = new ArrayList<>(files.length);
			for (File f : files) {
				if (f.getName().endsWith(".json") && f.isFile()) {
					String name = f.getName();
					ret.add(new TableEntry(name.substring(0, name.length()-5))); // remove the .json ending
				}
			}
			return ret;
		} else {
			return null;
		}
	}

	@Override
	public Schema getRemoteSchemaOrFail(String name) throws IOException {
		HanaBusinessObject n1 = HanaBusinessObject.readDefinition(null, null, name, null, bopath);
		try {
			return n1.getAvroSchema();
		} catch (SchemaException e) {
			throw new ConnectorRuntimeException("Schema cannot be parsed", e, null, null);
		}
	}

	public HanaBusinessObject getBusinessObject(String name) throws IOException {
		HanaBusinessObject n1 = HanaBusinessObject.readDefinition(null, null, name, null, bopath);
		return n1;
	}

	public File getBusinessObjectDirectory() {
		return bopath;
	}
	
	/* private void addNodeToIndex(List<BrowsingNode> nodes) {
		
		if (nodes != null) {
			for (BrowsingNode c : nodes) {
				nameindex.put(c.getNodeid(), c);
				addNodeToIndex(c.getChildren());
			}
		}
	} */
	
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
	
	/* public List<BrowsingNode> getHanaTableTree(String parentnodeid) throws ConnectorRuntimeException {
		if ( tables == null) {
			String sql = "select t.tabname, c.ddtext from \"" + getConnectionProperties().getSourceSchema() + "\".DD02L t "
					+ "left outer join \"" + getConnectionProperties().getSourceSchema() + "\".DD02T c on (c.tabname = t.tabname and c.ddlanguage = 'E')"
					+ "where t.tabclass = 'TRANSP' "
					+ "order by 1";
			try (PreparedStatement stmt = conn.prepareStatement(sql);) {
				ResultSet rs = stmt.executeQuery();
				String oldname = "";
				double weight = 0;
				List<BrowsingNode> sortedlist = new ArrayList<>();
				while (rs.next()) {
					String newname = rs.getString(1);
					weight += calcdistance(newname, oldname);
					BrowsingNode b = new BrowsingNode(newname, newname, rs.getString(2), weight);
					b.setExpandable(true);
					b.setImportable(true);
					sortedlist.add(b);
					oldname = newname;
				}
				tables = BrowsingNode.addAllSorted(sortedlist, 50);
				nameindex = new HashMap<>();
				addNodeToIndex(tables);
			} catch (SQLException e) {
				throw new ConnectorRuntimeException("Reading all tables of the ABAP schema failed", e, 
						"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
			}
		}
		List<BrowsingNode> ret = new ArrayList<>();
		if (parentnodeid == null) {
			// return the root list without children
			for (BrowsingNode b : tables) {
				BrowsingNode n = new BrowsingNode(b.getNodeid(), b.getDisplayname(), b.getDescription(), 0);
				n.setExpandable(b.isExpandable());
				n.setImportable(b.isImportable());
				n.setChildren(Collections.singletonList(new BrowsingNode("--expand--", "--expand--", null, 0)));
				ret.add(n);
			}
		} else {
			BrowsingNode root = nameindex.get(parentnodeid);
			if (root != null && root.getChildren() != null) {
				for (BrowsingNode b : root.getChildren()) {
					BrowsingNode n = new BrowsingNode(b.getNodeid(), b.getDisplayname(), b.getDescription(), 0);
					n.setChildren(Collections.singletonList(new BrowsingNode("--expand--", "--expand--", null, 0)));
					n.setExpandable(true);
					n.setImportable(b.isImportable());
					ret.add(n);
				}
			} else {
				// read foreign keys to physical (transparent) tables
				String sql = "select c.tabname, c.fieldname, p.fieldname from \"" + getConnectionProperties().getSourceSchema() + "\".DD08L c "
						+ "join \"" + getConnectionProperties().getSourceSchema() + "\".DD02L t on (t.tabname = c.tabname and t.tabclass = 'TRANSP') "
						+ "left join \"" + getConnectionProperties().getSourceSchema() + "\".DD03L p on (p.tabname = t.tabname and p.keyflag = 'X' and p.position = '0002')"
						+ "where c.checktable = ?"
						+ "order by 1";
				try (PreparedStatement stmt = conn.prepareStatement(sql);) {
					stmt.setString(1, parentnodeid);
					ResultSet rs = stmt.executeQuery();
					while (rs.next()) {
						String tablename = rs.getString(1);
						String fieldname = rs.getString(2);
						BrowsingNode b = new BrowsingNode(tablename, tablename + " (" + fieldname + ")", fieldname, 0);
						b.setImportable(true);
						b.setExpandable(true);
						b.addJoinCondition("MANDT", "MANDT");
						b.addJoinCondition(fieldname, rs.getString(3));
						b.setChildren(Collections.singletonList(new BrowsingNode("--expand--", "--expand--", null, 0)));
						ret.add(b);
					}
				} catch (SQLException e) {
					throw new ConnectorRuntimeException("Reading all tables of the ABAP schema failed", e, 
							"Execute the sql as Hana user \"" + getConnectionProperties().getUsername() + "\"", sql);
				}
			}
		}
		return ret;
	} */
	
	public Connection getConnection() {
		return conn;
	}

	/* private double calcdistance(String newname, String oldname) {
		int min = Math.min(newname.length(), oldname.length());
		int pos = 0;
		while (pos < min && newname.charAt(pos) == oldname.charAt(pos)) {
			pos++;
		}
		if (pos == min) {
			return 1;
		} else {
			// int diff = Math.abs(newname.charAt(pos) - oldname.charAt(pos));
			return 256/(pos+1);
		}
	} */
	
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
			this.schemaname = NameEncoder.encodeName(tablename);
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
