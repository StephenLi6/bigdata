package cn.itcast.pyg.report.cn.itcast.pyg.tools;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.List;

/**
 * Kafka工具类,为了拿KafkaTemplate
 */

@Configuration
@EnableKafka
public class KafkaUtils {

  /*  #kafka的服务器地址
    kafka.producer.server=node01:9092,node02:9092,node03:9092
            #如果出现发送失败的情况，允许重试的次数
    kafka.producer.retries=0
            #每个批次发送多大的数据
    kafka.producer.batch.size=4096
            #消息延迟发送的毫秒数，目的是为了等待多个消息，在同一批次发送，减少网络请求。
    kafka.producer.linger=100
            #生产者等待发送到kafka的消息队列占用内容的大小。
    kafka.producer.buffer.memory=40960*/

    @Value("${kafka.producer.server}")
    private String servers;

    @Value("${kafka.producer.retries}")
    private int retries;

    @Value("${kafka.producer.batch.size}")
    private int batchSize;

    @Value("${kafka.producer.linger}")
    private int linger;

    @Value("${kafka.producer.buffer.memory}")
    private int bufferMemory;


    @Bean
    public KafkaTemplate<String, String> getKafkaTemplate(){
        //使用Map封装Kafka配置
        HashMap<String, Object> map = new HashMap<>();

        map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        map.put(ProducerConfig.RETRIES_CONFIG, retries);
        map.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        map.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        map.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);

        //kafka的传输效率高,默认Java的序列化效率很低
        map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);


        DefaultKafkaProducerFactory factory = new DefaultKafkaProducerFactory<>(map);
        //创建KafkaTemplate对象
        return new KafkaTemplate<String, String>(factory);
    }


}
