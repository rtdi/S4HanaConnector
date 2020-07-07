package io.rtdi.bigdata.s4hanaconnector;

import io.rtdi.bigdata.connector.properties.ConnectionProperties;

public class S4HanaConnectionProperties extends ConnectionProperties {

	private static final String JDBCURL = "s4hana.jdbcurl";
	private static final String USERNAME = "s4hana.username";
	private static final String PASSWORD = "s4hana.password";
	private static final String SOURCESCHEMA = "s4hana.sourceschema";

	public S4HanaConnectionProperties(String name) {
		super(name);
		properties.addStringProperty(JDBCURL, "JDBC URL", "The JDBC URL to use for conencting to the S/4Hana system", "sap-icon://target-group", "jdbc:sap://localhost:3xx15/yy", true);
		properties.addStringProperty(USERNAME, "Username", "Hana database username", "sap-icon://target-group", null, true);
		properties.addPasswordProperty(PASSWORD, "Password", "Password", "sap-icon://target-group", null, true);
		properties.addStringProperty(SOURCESCHEMA, "Source database schema", "Database schema of the source tables", "sap-icon://target-group", null, true);
	}

	public String getJDBCURL() {
		return properties.getStringPropertyValue(JDBCURL);
	}
	
	public String getUsername() {
		return properties.getStringPropertyValue(USERNAME);
	}
	
	public String getPassword() {
		return properties.getPasswordPropertyValue(PASSWORD);
	}
	
	public String getSourceSchema() {
		return properties.getStringPropertyValue(SOURCESCHEMA);
	}
}
