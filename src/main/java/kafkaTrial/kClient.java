//package kafkaTrial;
//
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.TopicPartition;
//
//import java.util.Arrays;
//import java.util.Properties;
//
//import static kafkaTrial.kSettings.kfkAddr;
//import static kafkaTrial.kSettings.topicString;
//
///**
// * Created by zhou1 on 2018/11/13.
// */
//public class kClient {
//    private static KafkaConsumer<String, String> consumer;
//    private final static String TOPIC = topicString;
//    public kClient(){
//        Properties props = new Properties();
//        props.put("bootstrap.servers", kfkAddr);
//        //每个消费者分配独立的组号
//        props.put("group.id", "test2");
//        //如果value合法，则自动提交偏移量
//        props.put("enable.auto.commit", "true");
//        //设置多久一次更新被消费消息的偏移量
//        props.put("auto.commit.interval.ms", "1000");
//        //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
//        props.put("session.timeout.ms", "30000");
//        //自动重置offset
//        props.put("auto.offset.reset","earliest");
//        props.put("key.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer",
//                "org.apache.kafka.common.serialization.StringDeserializer");
//        consumer = new KafkaConsumer<String, String>(props);
//    }
//
//    public void consume(){
//        consumer.subscribe(Arrays.asList(TOPIC));
//        while (true) {
//            ConsumerRecords<String, String> records = consumer.poll(100);
//            for (ConsumerRecord<String, String> record : records){
//                System.out.printf("offset = %d, key = %s, value = %s",record.offset(), record.key(), record.value());
//                System.out.println();
//            }
//        }
//    }
//
//    public static void main(String[] args) {
//        new kClient().consume();
//    }
//}
