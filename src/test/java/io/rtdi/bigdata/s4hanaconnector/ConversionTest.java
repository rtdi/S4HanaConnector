package io.rtdi.bigdata.s4hanaconnector;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConversionTest {
	private static final DecimalConversion DECIMAL_CONVERTER = new DecimalConversion();

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		BigDecimal d = BigDecimal.valueOf(3.1415);
		Decimal decimaltype = LogicalTypes.decimal(7, 4);
		ByteBuffer buffer = DECIMAL_CONVERTER.toBytes(d, null, decimaltype);
		System.out.println(DECIMAL_CONVERTER.fromBytes(buffer, null, decimaltype).toString());
		BigDecimal n = DECIMAL_CONVERTER.fromBytes(buffer, null, decimaltype);
		System.out.println(n.toString());
	}

}
