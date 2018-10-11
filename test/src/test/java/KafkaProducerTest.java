/**
 * kafka生产者测试
 *
 * @author sgr
 * @create 2018-10-11 15:19
 **/

import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;


public class KafkaProducerTest {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "json";
        Producer<String, String> procuder = new KafkaProducer<String,String>(props);


        //列出topic的相关信息
        List<PartitionInfo> partitions = procuder.partitionsFor(topic);
        for(PartitionInfo p:partitions) {
            System.out.println(p);
        }

        //生产者发送消息


//        String message = "{\"date_time\": \"2018-10-09 12:30:01\", \"name\": \"dog\", \"age\": 5, \"sb\": false, \"parms\": {\"name2\": \"mon\", \"age2\": 10}, \"id\": 48}";
//        String message = "";
//      String  message = "{\"date_time\": 2018-10-09 12:30:01, \"name\": \"dog\", \"age\": 5, \"sb\": false, \"parms\": {\"name2\": \"mon\", \"age2\": 10}, \"id\": 48}";
      String  message = "{\"name\": \"dog\", \"age\": 5, \"sb\": false, \"parms\": {\"name2\": \"mon\", \"age2\": 10}, \"id\": 48}";
        ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, message);
        procuder.send(msg);


        System.out.println("send message over.");
        procuder.close(100,TimeUnit.MILLISECONDS);
    }
}
