package com.john.avro;

import com.john.core.MDEvent;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.io.IOException;

public class MDEventAvroDeserializer implements DeserializationSchema<MDEvent> {

	private transient DatumReader<MDEvent> reader;

	@Override
	public MDEvent deserialize(byte[] bytes) throws IOException {
		if (reader == null)
			initReader();

		Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
		return reader.read(null, decoder);
	}

	@Override
	public boolean isEndOfStream(MDEvent mdEvent) {
		return false;
	}

	@Override
	public TypeInformation<MDEvent> getProducedType() {
		return TypeExtractor.getForClass(MDEvent.class);
	}

	private void initReader() {
		reader = new SpecificDatumReader<>(MDEvent.class);
	}

}
