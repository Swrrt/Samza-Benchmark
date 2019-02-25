package generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/*
    Reading AOL search data (AOL_search_data_leak_2006.zip) and send them to Kafka topic.
    Push all data to Kafka topics before running stream application
*/
public class PerformanceWorkloadGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AOLgenerator.class);
    private final String outputTopic;
    private final String bootstrapServer;
    private long limit = 5000000;
    public PerformanceWorkloadGenerator(){
        outputTopic = "AOLraw";
        bootstrapServer = "yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096";
        limit = 5000000; //baseline
    }
    public PerformanceWorkloadGenerator(String topic, String bootstrapServer, long limit){
        outputTopic = topic;
        this.bootstrapServer = bootstrapServer;
        this.limit = limit;
    }

    public void generate(String file)throws InterruptedException{
        Properties props = setProps();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        BufferedReader br = null;
        FileReader fr = null;
        long lline = 0, line = 0, time = System.nanoTime(), ltime = time, interval = 2000000l, signal_interval = 3000000000l;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String curLine = br.readLine(); //Skip the first line of the file
            while ((curLine = br.readLine()) != null) {
                processAOLformat(curLine, producer);
                line++;
                time = System.nanoTime();
                if(time - ltime >= signal_interval){
                    System.out.printf("lines: %d", line - lline);
                    System.out.printf("time: %.3f", (time - ltime)/1000000000.0);
                    ltime = time;
                    lline = line;
                }
                if(line >= limit)break;
            }
            producer.close();
            System.out.printf("Output Complete: %d records", line);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public Properties setProps(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrapServer);
        prop.put("client.id", "AOLgenerator");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return prop;
    }
    public void processAOLformat(String line, KafkaProducer<String, String> producer){
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, line);
        producer.send(record);
    }
    public static void main(String[] args)throws InterruptedException{
        PerformanceWorkloadGenerator generator = new PerformanceWorkloadGenerator();
        String file = args[0];
        if(args.length > 1){
            generator = new PerformanceWorkloadGenerator(args[0], args[1], Long.parseLong(args[2]));
            file = args[3];
        }
        generator.generate(file);
    }
}
