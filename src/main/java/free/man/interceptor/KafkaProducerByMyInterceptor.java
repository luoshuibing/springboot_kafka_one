package free.man.interceptor;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/18 0:26
 **/
public class KafkaProducerByMyInterceptor {

    private static final String brokerList="192.168.149.20:9092";

    public static final String topicName="freeman";

    public static void main(String[] args) {
        Properties properties=new Properties();
        //设置key序列化器
        // properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //设置重试次数
        properties.put(ProducerConfig.RETRIES_CONFIG,3);
        //设置值序列化器
        // properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //设置拦截器
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MyProducerInterceptor.class.getName());
        //设置集群地址
        // properties.put("bootstrap.servers",brokerList);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        KafkaProducer<String,String> producer=new KafkaProducer<String,String>(properties);
        String s="hello kafka ,我是第一个消息1";
        ProducerRecord<String,String> record=new ProducerRecord<>(topicName,"kafka-demo",s);
        try {
            //异步发送
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        System.out.println("分区："+metadata.partition()+"===偏移量："+metadata.offset());
                    }else{
                        System.out.println(exception.getMessage());
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
