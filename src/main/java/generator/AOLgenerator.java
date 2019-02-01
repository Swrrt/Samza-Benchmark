package generator;

import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.sys.Prop;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

/*
    Reading AOL search data (AOL_search_data_leak_2006.zip) and send them to Kafka topic.
    Could start multiple generator on different host.
*/
public class AOLgenerator {
    private static final String outputTopic = "StreamBenchInput";
    private static final String bootstrapServer = "yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096";
    public static void main(String[] arg){
        BufferedReader br = null;
        FileReader fr = null;
        int n = arg.length;
        for(int i = 0; i<n;i++){
            try{
                fr = new FileReader(arg[i]);
                br = new BufferedReader(fr);
                Properties props = setProps();
                Producer<String, String> producer = new KafkaProducer<String, String>(props);
                String curLine = br.readLine(); //Skip the first line of the file
                while((curLine = br.readLine())!=null){
                    processAOLformat(curLine, producer);
                }
                producer.close();
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }
    public static Properties setProps(){
        Properties prop = new Properties();
        prop.put("bootstrap.server", bootstrapServer);
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return prop;
    }
    public static void processAOLformat(String line, Producer<String, String> producer){
        String [] items = line.split("\t");
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, items[0], line);
        producer.send(record);
    }
}
