package free.man.seek;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/18 15:08
 **/
public class SeekDemo {

    private static final String brokerList = "192.168.149.20:9092";

    public static final String topicName = "freeman";

    public static final String groupId = "group.demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        //设置key序列化器
        // properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置值序列化器
        // properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置集群地址
        // properties.put("bootstrap.servers",brokerList);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        // properties.put("group.id",groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //指定对应的客户端，默认为空，如果不设置kafkaconsumer会自动生成一个字符串
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "cosumer01");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topicName));
        Set<TopicPartition> assignment = new HashSet<>();
        while(assignment.isEmpty()){
            consumer.poll(Duration.ofMillis(110));
            //获取消费者的分区
            assignment = consumer.assignment();
        }
        System.out.println(assignment);
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition,10);
        }
        // consumer.seek(new TopicPartition(topicName,0),10);
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset()+"|"+record.value()+"|"+record.partition());
            }
        }

    }
}
