package iie.hadoop.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class Consumer extends Thread {
	public static volatile long count = 0;

	private final ConsumerConnector consumer;
	private final String topic;

	public Consumer(String topic, String zookeeper) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", topic);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));
		this.topic = topic;
	}

	public void run() {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			it.next().message();
			++count;
		}
	}

	public static void main(String[] args) throws InterruptedException {
		String topic = "default";
		String zookeeper = "localhost:2181/kafka";
		int threadNum = 1;

		for (int i = 0; i < args.length; ++i) {
			switch (args[i]) {
			case "--topic":
				topic = args[++i];
				break;
			case "--zookeeper":
				zookeeper = args[++i];
				break;
			case "--thread-num":
				threadNum = Integer.valueOf(args[++i]);
				break;
			default:
				break;
			}
		}
		for (int i = 0; i < threadNum; ++i) {
			new Consumer(topic, zookeeper).start();
		}

		Thread t = new Thread() {
			public void run() {
				long save = Consumer.count;
				try {
					while (true) {
						sleep(1 * 1000);
						long temp = Consumer.count;
						System.out.println((temp - save) / 10000.0);
						save = temp;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		};
		t.start();
		t.join();
	}
}