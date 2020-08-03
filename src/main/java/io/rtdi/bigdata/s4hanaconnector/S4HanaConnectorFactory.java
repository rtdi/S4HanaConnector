package io.rtdi.bigdata.s4hanaconnector;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import io.rtdi.bigdata.connector.connectorframework.BrowsingService;
import io.rtdi.bigdata.connector.connectorframework.ConnectorFactory;
import io.rtdi.bigdata.connector.connectorframework.IConnectorFactoryProducer;
import io.rtdi.bigdata.connector.connectorframework.Producer;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ProducerInstanceController;
import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;

public class S4HanaConnectorFactory extends ConnectorFactory<S4HanaConnectionProperties> 
implements IConnectorFactoryProducer<S4HanaConnectionProperties, S4HanaProducerProperties> {

	public S4HanaConnectorFactory() {
		super("S4Connector");
	}

	@Override
	public Producer<S4HanaConnectionProperties, S4HanaProducerProperties> createProducer(ProducerInstanceController instance) throws IOException {
		return new S4HanaProducer(instance);
	}

	@Override
	public S4HanaConnectionProperties createConnectionProperties(String name) throws PropertiesException {
		return new S4HanaConnectionProperties(name);
	}

	@Override
	public S4HanaProducerProperties createProducerProperties(String name) throws PropertiesException {
		return new S4HanaProducerProperties(name);
	}

	@Override
	public BrowsingService<S4HanaConnectionProperties> createBrowsingService(ConnectionController controller) throws IOException {
		return new S4HanaBrowse(controller);
	}

	@Override
	public boolean supportsBrowsing() {
		return true;
	}

	static Connection getDatabaseConnection(S4HanaConnectionProperties props) throws ConnectorCallerException {
		try {
			return getDatabaseConnection(props.getJDBCURL(), props.getUsername(), props.getPassword());
		} catch (SQLException e) {
			throw new ConnectorCallerException("Failed to establish a database connection", e, null, props.getJDBCURL());
		}
	}
	
	static Connection getDatabaseConnection(String jdbcurl, String user, String passwd) throws SQLException {
        try {
            Class.forName("com.sap.db.jdbc.Driver");
            return DriverManager.getConnection(jdbcurl, user, passwd);
        } catch (ClassNotFoundException e) {
            throw new SQLException("No Hana JDBC driver library found");
        }
	}

}
