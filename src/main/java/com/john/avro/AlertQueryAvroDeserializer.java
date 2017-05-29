package com.john.avro;

import com.john.core.AlertQuery;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;

public class AlertQueryAvroDeserializer implements DeserializationSchema<AlertQuery> {

	private transient DatumReader<AlertQuery> reader;

	@Override
	public AlertQuery deserialize(byte[] bytes) throws IOException {
		if (reader == null)
			initReader();

		Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
		return reader.read(null, decoder);
	}

	@Override
	public boolean isEndOfStream(AlertQuery t) {
		return false;
	}

	private void initReader() {
		 reader = new SpecificDatumReader<>(AlertQuery.class);
	}

	@Override
	public TypeInformation<AlertQuery> getProducedType() {
		return TypeExtractor.getForClass(AlertQuery.class);
	}
}
