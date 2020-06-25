package free.man.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/18 0:17
 **/
public class MyProducerInterceptor implements ProducerInterceptor<String,String> {

    private volatile long sendSuccess=0;

    private volatile long sendFailure=0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue="prefix-"+record.value();
        return new ProducerRecord<>(record.topic(),record.partition(),record.timestamp(),record.key(),modifiedValue,record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if(exception==null){
            sendSuccess++;
        }else{
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio=(double)sendSuccess/(sendSuccess+sendFailure);
        System.out.println("发送成功率="+String.format("%f",successRatio*100)+"%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
