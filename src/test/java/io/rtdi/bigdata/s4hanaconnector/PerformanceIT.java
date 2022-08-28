package io.rtdi.bigdata.s4hanaconnector;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.Collections;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.pipeline.foundation.utils.IOUtils;
import io.rtdi.bigdata.connector.properties.PipelineConnectionProperties;
import io.rtdi.bigdata.pipelinetest.PipelineTest;

public class PerformanceIT {
	private ConnectorController connector;
	private static final String TABLENAME = "VBAK";
	private static final String SCHEMAREGISTRYNAME = "io.rtdi.VBAK";
	private File tempdir = null;
	private Logger logger;
	private Connection dbconn;
	private SQLCommands sqlcommands;

	@Before
	public void setUp() throws Exception {
		Configurator.setAllLevels(LogManager.getRootLogger().getName(), Level.DEBUG);
		tempdir = Files.createTempDirectory(null).toFile();
		logger = LogManager.getLogger(this.getClass().getName());

		logger.info("setup of environment is started");
		PipelineTest api = new PipelineTest();
		api.setConnectionProperties(new PipelineConnectionProperties("EMPTY"));
		
		S4HanaConnectionProperties connprops = new S4HanaConnectionProperties("test");
		connprops.setJDBCURL("jdbc:sap://hana2:39015/HXE");
		connprops.setUsername("s4hana_connect");
		String password = System.getenv("TEST_PASSWD");
		connprops.setPassword(password);
		connprops.setSourceSchema("S4HANA_SOURCE");
		
		S4HanaProducerProperties producerprops = new S4HanaProducerProperties("testproducer");
		producerprops.setSourceSchemas(Collections.singletonList(SCHEMAREGISTRYNAME));
		producerprops.setPollInterval(1);
		
		connector = new ConnectorController(new S4HanaConnectorFactory(), tempdir.getAbsolutePath(), null);
		connector.setAPI(api);
		ConnectionController connection = connector.addConnection(connprops);
		connection.addProducer(producerprops);
		
		S4HanaBrowse browser = (S4HanaBrowse) connection.getBrowser();
		dbconn = browser.getConnection();
		sqlcommands = new SQLCommands(dbconn, logger);
		sqlcommands.dropTable("S4HANA_SOURCE", "VBAK");
		sqlcommands.dropTable("S4HANA_CONNECT", "PKLOG");
		sqlcommands.executeScript("VBAK.sql");
		S4HanaTableMapping entity = new S4HanaTableMapping(
				SCHEMAREGISTRYNAME,
				connprops.getUsername(),
				connprops.getSourceSchema(),
				TABLENAME,
				"L1",
				connprops.getSourceSchema(),
				browser.getConnection());
		entity.write(browser.getBusinessObjectDirectory());
		logger.info("setup of environment is completed");
	}

	@After
	public void tearDown() throws Exception {
		if (connector != null) {
			connector.disableController();
		}
		if (dbconn != null) {
			dbconn.close();
		}
		deleteTempDir();
	}
	
	protected void deleteTempDir() {
		if (tempdir != null) {
			try {
				IOUtils.deleteDirectory(tempdir);
			} catch (IOException e) {
				System.out.println("Cannot delete the temporary test directory");
			}
		}
	}
	
	protected void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (InterruptedException e) {
		}
	}

	@Test
	public void test() {
		try {
			logger.info("Test starts");
			int maxseconds = 20;
			connector.startController();
			do {
				sleep(1000);
				if (maxseconds % 6 == 0) {
					sqlcommands.executeScript("VBAK_change.sql");
				}
				maxseconds--;
			} while (connector.getErrorListRecursive().size() == 0 && maxseconds > 0);
			System.out.println("Row Processed: " + connector.getRowsProcessed());
			if (connector.getErrorListRecursive().size() != 0) {
				System.out.println(connector.getErrorListRecursive());
				fail(connector.getErrorListRecursive().toString());
			}
			logger.info("Test finished");
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
