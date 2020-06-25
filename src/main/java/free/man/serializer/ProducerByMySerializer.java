package free.man.serializer;

import free.man.pojo.Company;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/17 23:45
 **/
public class ProducerByMySerializer {

    private static final String brokerList="192.168.149.20:9092";

    public static final String topicName="freeman";

    public static void main(String[] args) {
        Properties properties=new Properties();
        //设置key序列化器
        // properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MySerializer.class.getName());
        //设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        //设置值序列化器
        // properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,MySerializer.class.getName());
        //设置集群地址
        // properties.put("bootstrap.servers",brokerList);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        KafkaProducer<String,Company> producer=new KafkaProducer<String,Company>(properties);
        Company company = Company.builder().name("自由人").address("北京").build();
        ProducerRecord<String,Company> record=new ProducerRecord<>(topicName,company);
        try {
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println("分区为："+recordMetadata.partition());
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }

}
