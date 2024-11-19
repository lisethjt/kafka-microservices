package com.kafka.streams;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class Consumidor {

	public static void main(String[] args) {
		Properties props = new Properties();
		// Clases para deserialzar clave y valor
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// lista de host y puertos para coenxion inicial
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// Identificador del grupo de consumidores al que pertenece el consumidor
		props.put("group.id", "grupo1");
		// true, ofset del cosumidor se escribira periodicamente en segundo plano
		props.put("enable.auto.commit", "true");
		// frecuencia en milisegundos
		props.put("auto.commit.interval.ms", "1000");
		// Cantidad minima de datos que quiere recibir del broker cuando lee regstros
		props.put("fetch.min.bytes", "1");
		// cuanto esperar en caso de que el broker no tenga suficientes datos
		props.put("fetch.max.wait.ms", "500");
		// controla el max numero de bytes que se devolvera a cada particion asignada al
		// consumidor
		props.put("max.partition.fetch.bytes", "1048576");
		// controla el tiempo que un consumidor se considera funcional
		props.put("session.timeout.ms", "10000");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		try {
			// subcripcion al topic
			consumer.subscribe(Collections.singleton("topic-test"));
			while (true) {
				// consumo de mensajes
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
				records.forEach(record -> {
					System.out.println("****** Topic: " + record.topic());
					System.out.print("Partition: " + record.partition() + ",");
					System.out.print("Key:" + record.key() + ", ");
					System.out.println("Value: " + record.value() + ", ");
				});
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			consumer.close();
		}

	}
}