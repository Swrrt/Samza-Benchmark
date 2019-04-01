package generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/*
    Reading AOL search data (AOL_search_data_leak_2006.zip) and send them to Kafka topic.
    Could start multiple generator on different host.
*/
public class DumbGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AOLgenerator.class);
    private final String outputTopic;
    private final String bootstrapServer;
    private final boolean isKeyPartition;
    public DumbGenerator(){
        outputTopic = "DumbRaw";
        bootstrapServer = "yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096";
        isKeyPartition = false;
    }
    public DumbGenerator(String topic, String bootstrapServer, String input3){
        outputTopic = topic;
        this.bootstrapServer = bootstrapServer;
        if(input3.equals("true") || input3.equals("True") || input3.equals("TRUE"))isKeyPartition = true;
        else isKeyPartition = false;
    }

    public void generate(HashMap<String, Integer> dumbStrings, int speed)throws InterruptedException{
        Properties props = setProps();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        long lline = 0, line = 0, time = System.nanoTime(), ltime = time, interval = 1000000000l/speed /* Number of records per second*/, signal_interval = 3000000000l;
        for(Map.Entry<String, Integer> entry: dumbStrings.entrySet()){
            String curLine = entry.getKey();
            for(int times = 0 ;times <entry.getValue(); times++) {
                if (isKeyPartition) processAOLformat(curLine, producer);
                else processAOLformatWithKey(curLine, producer);
                line++;
                time = System.nanoTime();
                if (time - ltime >= signal_interval) {
                    System.out.printf("lines: %d, ", line - lline);
                    System.out.printf("time: %.3f\n\n", (time - ltime) / 1000000000.0);
                    ltime = time;
                    lline = line;
                }
                while (System.nanoTime() - time < interval) ;         /* Control the throughput of producing*/
            }
            //if(line >= 10000)break;
        }
        producer.close();
    }
    public Properties setProps(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrapServer);
        prop.put("client.id", "AOLgenerator");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return prop;
    }
    // Round-Robin
    public void processAOLformat(String line, KafkaProducer<String, String> producer){
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, line);
        producer.send(record);
    }
    // With Key
    public void processAOLformatWithKey(String line, KafkaProducer<String, String> producer){
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, line.split("[\\\\s\\\\xA0]+")[0], line);
        producer.send(record);
    }
    public static void main(String[] args)throws InterruptedException{
        DumbGenerator generator = new DumbGenerator();
        generator = new DumbGenerator(args[0], args[1], args[2]);

        int n = Integer.parseInt(args[3]);// How many of partitions are heavy loaded
        int speed = Integer.parseInt(args[4]); //Generate speed

        HashMap<String, Integer> dumbStrings = new HashMap<>();
        for(int i=0;i<100;i++){
            StringBuilder str = new StringBuilder();

            str.append((char)(i%26+'a'));
            str.append(i);
            str.append((char)(i%26+'a'));
            str.append((char)(i%26+'a'));

            StringBuilder output = new StringBuilder();
            int times = 10;
            while(times>0){
                times--;
                output.append(str);
                output.append(' ');
            }
            if(i<n)dumbStrings.put(output.toString(), 50);
            else dumbStrings.put(output.toString(), 1);
        }
         /*
            Number of records generated per second.
            Limited by Kafka (Around 500000)
         */
        while (true) {
            generator.generate(dumbStrings, speed);
        }
    }
}
