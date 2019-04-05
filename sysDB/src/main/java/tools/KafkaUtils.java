package tools;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * Kafka工具类
 */
public class KafkaUtils {

    public static void send(String data){

        Properties properties = new Properties();

        properties.put("metadata.broker.list" , "node01:9092");
        properties.put("zookeeper.connect" , "node01:2181");
        properties.put("serializer.class" , StringEncoder.class.getName());

        ProducerConfig config = new ProducerConfig(properties);
        //Kafka消息发送
        Producer<String, String> producer = new Producer<String, String>(config);
        producer.send(new KeyedMessage<String, String>("canal", data));
    }
}
