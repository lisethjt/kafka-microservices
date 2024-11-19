package com.kafka.streams;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Productor {

	public static void main(String[] args) {
		Properties props = new Properties();
		// serializar clave y valor de los registros
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// Controla si es productor espera la confirmacion de escritura del broker o no.
		// En caso de que espera se debe definir el
		// numero de replicas antes de enviar la confirmacion: 0 - no espera
		// confirmacion del broker (no fiable)
		// 1 - espera confirmacipn cuando la replica lider recibe el mensaje, en caso de
		// que el mensaje no se pueda escribir
		// el productor recibe unmensaje y puede volver a intentarlo
		// all - el productor recibe confirmacion cuando todas las replicas reciban el
		// mensaje
		props.put("acks", "all");
		// Lista de host y puertos que usa el productor para establcer la conexion
		// inicial
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		// numero de reintentos en caso de fallo
		props.put("retries", 0);
		// tamaño del buffer que almacena eventos
		props.put("batch.size", 16384);
		// memoria disponible para el buffer del productor
		props.put("buffer.memory", 33554432);

		KafkaProducer<String, String> prod = new KafkaProducer<>(props);
		String topic = "topic-test";
		int partition = 0;
		String key = "testKey";
		String value = "testValue2";
		// metodo send es el responsable de enviar el mensaje, es asincrono
		prod.send(new ProducerRecord<String, String>(topic, partition, key, value));
		// envio sincrono: espera la confirmacion del broker. Es necesario implementar
		// un callback
		try {
			prod.send(new ProducerRecord<String, String>(topic, partition, key, value)).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
		prod.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception e) {
				if (e != null) {
					System.out.println("Send failed for record");
				}

			}
		});
		prod.close();
	}
}