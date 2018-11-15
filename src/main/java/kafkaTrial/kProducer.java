//package kafkaTrial;
//
//
//import org.apache.kafka.clients.producer.Callback;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.Producer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;
//
//import java.util.Properties;
//import java.util.concurrent.Future;
//
//import static kafkaTrial.kSettings.kfkAddr;
//import static kafkaTrial.kSettings.topicString;
//
///**
// * Created by zhou1 on 2018/11/13.
// */
//public class kProducer {
//
//    private static KafkaProducer<String, String> producer;
//    private final static String TOPIC = topicString;
//    public kProducer(){
//        Properties props = new Properties();
//        props.put("bootstrap.servers", kfkAddr);
////        props.put("acks", "all");
//        props.put("retries", 0);
//        props.put("batch.size", 16384);
//        props.put("linger.ms", 1);
//        props.put("buffer.memory", 33554432);
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
////        props.put("min.insync.replicas",3);
//        //设置分区类,根据key进行数据分区
//        producer = new KafkaProducer<String, String>(props);
//    }
//    public void produce(String key,String value){
//        Future f =  producer.send(new ProducerRecord<String, String>(TOPIC,key,value));
//        System.out.println(value);
//        producer.close();
//    }
//
//    public static void main(String[] args) {
//        kProducer k = new kProducer();
//        k.produce("hello","world");
//        k.produce("foo","bar");
//    }
//}
