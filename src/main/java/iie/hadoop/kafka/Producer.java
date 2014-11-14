package iie.hadoop.kafka;

import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producer extends Thread {
	public static volatile long count = 0;

	private final kafka.javaapi.producer.Producer<Long, byte[]> producer;
	private final String topic;
	private final int messageSize;

	public Producer(String topic, String brokerList, int messageSize) {
		Properties props = new Properties();
		props.put("metadata.broker.list", brokerList);
		producer = new kafka.javaapi.producer.Producer<Long, byte[]>(
				new ProducerConfig(props));
		this.topic = topic;
		this.messageSize = messageSize;
	}

	public void run() {
		KeyedMessage<Long, byte[]> message = new KeyedMessage<Long, byte[]>(
				topic, new byte[messageSize]);
		while (true) {
			producer.send(message);
			++count;
		}
	}

	public static void main(String[] args) throws InterruptedException {
		String topic = "default";
		String brokerList = "localhost:9092";
		int messageSize = 1024;
		int threadNum = 1;

		for (int i = 0; i < args.length; ++i) {
			switch (args[i]) {
			case "--topic":
				topic = args[++i];
				break;
			case "--broker-list":
				brokerList = args[++i];
				break;
			case "--message-size":
				messageSize = Integer.valueOf(args[++i]);
				break;
			case "--thread-num":
				threadNum = Integer.valueOf(args[++i]);
				break;
			default:
				break;
			}
		}
		for (int i = 0; i < threadNum; ++i) {
			new Producer(topic, brokerList, messageSize).start();
		}

		Thread t = new Thread() {
			public void run() {
				long save = Producer.count;
				try {
					while (true) {
						sleep(1 * 1000);
						long temp = Producer.count;
						if (temp == save) {
							break;
						}
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