package generator;

import org.apache.commons.csv.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/*
    Reading AOL search data (AOL_search_data_leak_2006.zip) and send them to Kafka topic.
    Could start multiple generator on different host.
*/
public class EnronGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AOLFixedGenerator.class);
    private final String outputTopic;
    private final String bootstrapServer;
    public EnronGenerator(){
        outputTopic = "EnronRaw";
        bootstrapServer = "yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096";
    }
    public EnronGenerator(String topic, String bootstrapServer){
        outputTopic = topic;
        this.bootstrapServer = bootstrapServer;
    }

    public long generate(String file, long numberToGenerate, long speed)throws InterruptedException{
        Properties props = setProps();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        Reader reader = null;
        CSVParser csvParser = null;
        long lline = 0, line = 0, time = System.nanoTime(), ltime = time, interval = 1000000000l/speed /* Number of records per second*/, signal_interval = 3000000000l;
        try {
            reader = Files.newBufferedReader(Paths.get(file));
            csvParser = new CSVParser(reader, CSVFormat.DEFAULT);
            for(CSVRecord record : csvParser) {
                String fileName = record.get(0);
                String message = record.get(1);
                processEnronformat(message, producer);
                line++;
                time = System.nanoTime();
                if(time - ltime >= signal_interval){
                    System.out.printf("lines: %d, ", line - lline);
                    System.out.printf("time: %.3f\n\n", (time - ltime)/1000000000.0);
                    ltime = time;
                    lline = line;
                }
                while(System.nanoTime() - time < interval);         /* Control the throughput of producing*/
                //if(line >= 10000)break;
                if(line >= numberToGenerate)break;
            }
            producer.close();
            return line;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line;
    }
    public Properties setProps(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrapServer);
        prop.put("client.id", "EnronGenerator");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return prop;
    }
    public String processEnronEmail(String line){

        return line;
    }
    public void processEnronformat(String line, KafkaProducer<String, String> producer){
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, processEnronEmail(line));
        producer.send(record);
    }
    public static void main(String[] args)throws InterruptedException{
        EnronGenerator generator = new EnronGenerator();
        generator = new EnronGenerator(args[0], args[1]);
        int n = args.length - 4;
        String [] files = new String[n];
        for(int i=0 ; i<n ; i++){
            files[i] = args[i+4];
        }
        /*
            Keep generating from the first file to the last file until reach the target number
            If target number is -1, generate infinitely.
        * */
        long numberToGenerate = Long.parseLong(args[2]);
        /*
            Number of records generated per second.
            Limited by Kafka (Around 500000)
         */
        long speed = Long.parseLong(args[3]);
        if(numberToGenerate == -1) {
            while (true) {
                for (int i = 0; i < n; i++)
                    generator.generate(files[i], -1, speed);
            }
        }else{
            while (numberToGenerate > 0) {
                for (int i = 0; i < n; i++)
                    numberToGenerate -= generator.generate(files[i], numberToGenerate, speed);
            }
        }
    }
}
