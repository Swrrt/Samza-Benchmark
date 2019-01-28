package generator;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.LinkedList;
import java.util.List;

/*
    Reading AOL search data (AOL_search_data_leak_2006.zip) and send them to Kafka topic.
    Could start multiple generator on different host.
*/
public class AOLgenerator {
    private final String outputTopic = "StreamBenchInput";
    public static void main(String[] arg){

    }
}
