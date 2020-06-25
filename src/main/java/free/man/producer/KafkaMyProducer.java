package free.man.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/4 22:33
 **/
public class KafkaMyProducer {

    private static final String brokerList = "192.168.149.20:9092";

    public static final String topicName = "freeman";

    public static void main(String[] args) {
        Properties properties = new Properties();
        //设置key序列化器
        // properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);
        //设置值序列化器
        // properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //设置集群地址
        // properties.put("bootstrap.servers",brokerList);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        try {
            //同步发送
            // Future<RecordMetadata> result = producer.send(record);
            // RecordMetadata data = result.get();
            // System.out.println("偏移量："+data.offset());
            // System.out.println("主题名称："+data.topic());
            // System.out.println("分区："+data.partition());
            //异步发送
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, 0,"kafka-demo", "hello kafka ,我是第一个消息");
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, 1, System.currentTimeMillis() - 10 * 1000, "kafka-demo", "hello kafka ,我是第一个消息");
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("分区：" + metadata.partition() + "===偏移量：" + metadata.offset());
                    }
                }
            });
            producer.send(record2, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        System.out.println("分区：" + metadata.partition() + "===偏移量：" + metadata.offset());
                    }
                }
            });
            System.out.println("=========");
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }

}
