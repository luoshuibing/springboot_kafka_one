package free.man.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/18 12:34
 **/
public class KafkaOffsetNotAutoCommitSyncConsumer {

    private static final String brokerList="192.168.149.20:9092";

    public static final String topicName="freeman";

    public static final String groupId="group.demo";

    public static void main(String[] args) throws Exception {
        Properties properties=new Properties();
        //设置key序列化器
        // properties.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //设置值序列化器
        // properties.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //设置集群地址
        // properties.put("bootstrap.servers",brokerList);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        // properties.put("group.id",groupId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //指定对应的客户端，默认为空，如果不设置kafkaconsumer会自动生成一个字符串
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG,"cosumer01");
        //设置非自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(properties);
        TopicPartition tp = new TopicPartition(topicName, 0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumedOffset=-1;
        while(true){
            //设置1秒监听一次
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(1000));
            if(records.isEmpty()){
                break;
            }
            List<ConsumerRecord<String, String>> patitionRecords = records.records(tp);
            lastConsumedOffset=patitionRecords.get(patitionRecords.size()-1).offset();
            consumer.commitSync();
        }
        System.out.println("消费端偏移量："+lastConsumedOffset);
        OffsetAndMetadata metaData = consumer.committed(tp);
        System.out.println("下一个消费从哪里开始的偏移量："+metaData.offset());
        long position = consumer.position(tp);
        System.out.println("下一个消费从哪里开始的偏移量："+position);
    }

}
