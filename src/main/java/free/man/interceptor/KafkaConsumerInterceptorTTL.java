package free.man.interceptor;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/18 15:53
 **/
public class KafkaConsumerInterceptorTTL implements ConsumerInterceptor<String,String> {

    public static final long EXPIRE_INTERVAL=10*1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        System.out.println("before:"+records);
        long now= System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String,String>>> newResult=new HashedMap();
        for (TopicPartition tp : records.partitions()) {
            List<ConsumerRecord<String,String>> oldRecords=records.records(tp);
            List<ConsumerRecord<String,String>> newRecords=new ArrayList<>();
            for (ConsumerRecord<String, String> oldRecord : oldRecords) {
                if(now-oldRecord.timestamp()<EXPIRE_INTERVAL){
                    newRecords.add(oldRecord);
                }
            }
            if(!newRecords.isEmpty()){
                newResult.put(tp,newRecords);
            }
        }
        return new ConsumerRecords<>(newResult);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp,offset)->{
            System.out.println(tp+"==="+offset.offset());
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
