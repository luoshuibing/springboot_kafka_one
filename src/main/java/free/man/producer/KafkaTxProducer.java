package free.man.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/19 12:15
 **/
public class KafkaTxProducer {

    private static final String brokerList = "192.168.149.20:9092";

    public static final String topicName = "freeman";

    public static final String transactionId="transactionId";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        //设置发送端事务处理
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,transactionId);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //初始化事务
        producer.initTransactions();
        //开启事务
        producer.beginTransaction();
        try {
            ProducerRecord<String, String> record1 = new ProducerRecord<>(topicName, "kafka事务消息1");
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, "kafka事务消息2");
            ProducerRecord<String, String> record3 = new ProducerRecord<>(topicName, "kafka事务消息3");
            ProducerRecord<String, String> record4 = new ProducerRecord<>(topicName, "kafka事务消息4");
            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.send(record4);
            System.out.println(1/0);
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            producer.abortTransaction();
        }
        producer.close();
    }

}
