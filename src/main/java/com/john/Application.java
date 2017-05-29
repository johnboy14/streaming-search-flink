package com.john;

import com.john.avro.AlertQueryAvroDeserializer;
import com.john.avro.MDEventAvroDeserializer;
import com.john.core.AlertQuery;
import com.john.core.MDEvent;
import com.john.core.MatchedEventSink;
import com.john.core.MergedType;
import com.john.operator.AlertQueryMapFunction;
import com.john.operator.LuwakSearchMapFunction;
import com.john.operator.MDEventMapFunction;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import uk.co.flax.luwak.QueryMatch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

public class Application {

	public static void main(String[] args) throws Exception {
		produceMockMDEvent();
		produceMockAlerts();
		execute();
	}

	private static void execute() throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

		//Consume Alert Queries
		DataStreamSource<AlertQuery> queryStream =
				environment.addSource(new FlinkKafkaConsumer08<>("streamAlerts", new AlertQueryAvroDeserializer(), getKafkaProperties()));

		//Consume MD Events
		DataStreamSource<MDEvent> mdEventStream =
				environment.addSource(new FlinkKafkaConsumer08<>("mdEvents2", new MDEventAvroDeserializer(), getKafkaProperties()));


		//Generalise Streams
		DataStream<MergedType> genericQueryStream = queryStream.map(new AlertQueryMapFunction());
		DataStream<MergedType> genericMDEventStream = mdEventStream.map(new MDEventMapFunction());


		DataStream<MergedType> mergedStream = genericQueryStream.union(genericMDEventStream);

		DataStream<QueryMatch> matchedEventStream =
				mergedStream.flatMap(new LuwakSearchMapFunction());


		matchedEventStream.addSink(new MatchedEventSink());

		//Execute
		environment.execute();

	}

	public static Properties getKafkaProperties() {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("zookeeper.connect", "localhost:2181");
		properties.put("group.id", "streaming-consumer");
		properties.put("auto.offset.reset", "latest");

		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		return properties;
	}


	private static void produceMockAlerts() {
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(getKafkaProperties());
		CompletableFuture.runAsync(() -> {
			int volume = 10;
			while (true) {
				try {
					Thread.sleep(2000);
					Map<String, String> queryMap = new HashMap<>();
					queryMap.put("volume", String.valueOf(volume));
					AlertQuery query = AlertQuery.newBuilder().setUserId("A/A").setQueryMap(queryMap).build();
					volume++;
					sendMessage(producer, query, "streamAlerts");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	private static void produceMockMDEvent() {
		KafkaProducer<String, byte[]> producer = new KafkaProducer<>(getKafkaProperties());
		CompletableFuture.runAsync(() -> {
			int volume = 10;
			while (true) {
				try {
					Thread.sleep(2000);
					Map<String, String> queryMap = new HashMap<>();
					queryMap.put("volume", String.valueOf(volume));
					MDEvent query = MDEvent.newBuilder()
							.setMsgId(String.valueOf(volume))
							.setVolume(String.valueOf(volume))
							.build();
					volume++;
					sendMessage(producer, query, "mdEvents2");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		});
	}

	private static void sendMessage(KafkaProducer<String, byte[]> producer, AlertQuery query, String topic) {
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, toBytes(query));
		producer.send(record);
	}

	private static void sendMessage(KafkaProducer<String, byte[]> producer, MDEvent query, String topic) {
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, toBytes(query));
		producer.send(record);
	}

	private static byte[] toBytes(AlertQuery query) {
		DatumWriter<AlertQuery> writer = new SpecificDatumWriter<>(AlertQuery.class);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
		try {
			writer.write(query, encoder);
			encoder.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return os.toByteArray();
	}

	private static byte[] toBytes(MDEvent query) {
		DatumWriter<MDEvent> writer = new SpecificDatumWriter<>(MDEvent.class);
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(os, null);
		try {
			writer.write(query, encoder);
			encoder.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return os.toByteArray();
	}
}
