package com.kafka.streams;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.TimeWindows;

public class ProductorStreams {

	public static void main(String[] args) {
		String TOPIC = "TopicName";
		final StreamsBuilder builder = new StreamsBuilder();
		builder.stream(TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
		.groupByKey()
		.windowedBy(TimeWindows.of(Duration.ofSeconds(28)))
		.reduce((value1, value2) -> value1 + value2)
		.toStream()
		.print(Printed.toSysOut());
		
		Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, props);
		streams.start();
	}
}