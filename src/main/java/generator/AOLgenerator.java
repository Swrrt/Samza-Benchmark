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
    Could start multiple generator on different host.
*/
public class AOLgenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AOLgenerator.class);
    private final String outputTopic;
    private final String bootstrapServer;
    public AOLgenerator(){
        outputTopic = "AOLraw";
        bootstrapServer = "yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096";
    }
    public AOLgenerator(String topic, String bootstrapServer){
        outputTopic = topic;
        this.bootstrapServer = bootstrapServer;
    }

    public long generate(String file, long numberToGenerate)throws InterruptedException{
        Properties props = setProps();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        BufferedReader br = null;
        FileReader fr = null;
        long lline = 0, line = 0, time = System.nanoTime(), ltime = time, interval = 1000000000l/100000 /* Number of records per second*/, signal_interval = 3000000000l;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String curLine = br.readLine(); //Skip the first line of the file
            while ((curLine = br.readLine()) != null) {
                processAOLformat(curLine, producer);
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
        AOLgenerator generator = new AOLgenerator();
        generator = new AOLgenerator(args[0], args[1]);
        int n = args.length - 3;
        String [] files = new String[n];
        for(int i=0 ; i<n ; i++){
            files[i] = args[i+3];
        }
        /*
            Keep generating from the first file to the last file until reach the target number
            If target number is -1, generate infinitely.
        * */
        long numberToGenerate = Long.parseLong(args[3]);
        if(numberToGenerate == -1) {
            while (true) {
                for (int i = 0; i < n; i++)
                    generator.generate(files[i], -1);
            }
        }else{
            while (numberToGenerate > 0) {
                for (int i = 0; i < n; i++)
                    numberToGenerate -= generator.generate(files[i], numberToGenerate);
            }
        }
    }
}
