package io.rtdi.bigdata.s4hanaconnector;

import java.io.File;
import java.util.List;

import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ProducerProperties;

public class S4HanaProducerProperties extends ProducerProperties {

	private static final String PRODUCER_TOPICNAME = "producer.topic";
	private static final String PRODUCER_POLLINTERVAL = "producer.pollinterval";
	private static final String PRODUCER_SOURCE_SCHEMAS = "producer.source";

	public S4HanaProducerProperties(String name) throws PropertiesException {
		super(name);
		properties.addStringProperty(PRODUCER_TOPICNAME, "Target Topic", null, null, name, true);
		properties.addIntegerProperty(PRODUCER_POLLINTERVAL, "Poll interval", "Poll every n seconds", null, 60, true);
		properties.addMultiSchemaSelectorProperty(PRODUCER_SOURCE_SCHEMAS, "Source tables", "List of source tables to create data for", null, true);
	}

	public S4HanaProducerProperties(File dir, String name) throws PropertiesException {
		super(dir, name);
	}

	public String getTopicName() {
		return properties.getStringPropertyValue(PRODUCER_TOPICNAME);
	}
	
	public int getPollInterval() {
		return properties.getIntPropertyValue(PRODUCER_POLLINTERVAL);
	}
	
	public List<String> getSourceSchemas() throws PropertiesException {
		return properties.getMultiSchemaSelectorValue(PRODUCER_SOURCE_SCHEMAS);
	}
	
}
