package free.man.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @Description
 * @Author liupeng
 * @Date 2020/6/18 17:09
 **/
public class MyAdminClientConfig {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // addTopicPartition();
        describeTopic();
    }

    //修改主题分区到4个
    public static void addTopicPartition() throws ExecutionException, InterruptedException {
        String brokerList="192.168.149.20:9092";
        String topic="freeman";
        Properties ps = getConfig(brokerList);
        AdminClient client=AdminClient.create(ps);
        NewPartitions newPartitions=NewPartitions.increaseTo(4);
        Map<String,NewPartitions> newPartitionsMap=new HashMap<>();
        newPartitionsMap.put(topic,newPartitions);
        CreatePartitionsResult result=client.createPartitions(newPartitionsMap);
        result.all().get();
        client.close();
    }

    //描述主题
    public static void describeTopic() throws ExecutionException, InterruptedException {
        String brokerList="192.168.149.20:9092";
        String topic="freeman";
        Properties ps = getConfig(brokerList);
        AdminClient client=AdminClient.create(ps);
        ConfigResource resource=new ConfigResource(ConfigResource.Type.TOPIC,topic);
        DescribeConfigsResult result=client.describeConfigs(Collections.singleton(resource));
        Config config = result.all().get().get(resource);
        System.out.println(config);
        client.close();
    }

    public static Properties getConfig(String brokerList){
        Properties ps=new Properties();
        ps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,brokerList);
        ps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG,3000);
        return ps;
    }

}
