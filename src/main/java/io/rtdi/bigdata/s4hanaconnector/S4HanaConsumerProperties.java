package io.rtdi.bigdata.s4hanaconnector;

import io.rtdi.bigdata.connector.pipeline.foundation.TopicName;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PropertiesException;
import io.rtdi.bigdata.connector.properties.ConsumerProperties;

public class S4HanaConsumerProperties extends ConsumerProperties {

	public S4HanaConsumerProperties(String name) throws PropertiesException {
		super(name);
		// TODO Auto-generated constructor stub
	}

	public S4HanaConsumerProperties(String name, TopicName topic) throws PropertiesException {
		super(name, topic);
		// TODO Auto-generated constructor stub
	}

	public S4HanaConsumerProperties(String name, String pattern) throws PropertiesException {
		super(name, pattern);
		// TODO Auto-generated constructor stub
	}

}
