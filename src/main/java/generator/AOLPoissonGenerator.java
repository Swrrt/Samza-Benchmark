package generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/*
    Reading AOL search data (AOL_search_data_leak_2006.zip) and send them to Kafka topic in Poisson process.
    Could start multiple generator on different host.
*/
public class AOLPoissonGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AOLFixedGenerator.class);
    private final String outputTopic;
    private final String bootstrapServer;
    private final boolean isKeyPartition;
    public AOLPoissonGenerator(){
        outputTopic = "AOLraw";
        bootstrapServer = "yy04:9092,yy05:9093,yy06:9094,yy07:9095,yy08:9096";
        isKeyPartition = false;
    }
    public AOLPoissonGenerator(String topic, String bootstrapServer, String input3){
        outputTopic = topic;
        this.bootstrapServer = bootstrapServer;
        if(input3.equals("true") || input3.equals("True") || input3.equals("TRUE"))isKeyPartition = true;
        else isKeyPartition = false;
    }

    public long generate(String file, long numberToGenerate, long speed)throws InterruptedException{
        Properties props = setProps();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        BufferedReader br = null;
        FileReader fr = null;
        Random rand = new Random();
        long lline = 0, line = 0, time = System.nanoTime(), ltime = time, interval = 1000000000l/speed /* Number of records per second*/, stdDeviation = interval / 5, signal_interval = 5000000000l;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String curLine = br.readLine(); //Skip the first line of the file
            while ((curLine = br.readLine()) != null) {
                if(isKeyPartition)processAOLformat(curLine, producer);
                else processAOLformatWithKey(curLine, producer);
                line++;
                time = System.nanoTime();
                if(time - ltime >= signal_interval){
                    System.out.printf("lines: %d, ", line - lline);
                    System.out.printf("time: %.3f\n\n", (time - ltime)/1000000000.0);
                    ltime = time;
                    lline = line;
                }
                double exponentialRandom = rand.nextGaussian()*stdDeviation + interval;
                while(System.nanoTime() - time < exponentialRandom);         /* Control the throughput of producing*/
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
    public long getPoissonRandom(double mean){
        Random r = new Random();
        double L = Math.exp(-mean);
        long k = 0;
        double p = 1.0;
        do {
            p = p * r.nextDouble();
            k++;
        } while (p > L);
        return k - 1;
    }

    public Properties setProps(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", bootstrapServer);
        prop.put("client.id", "AOLFixedGenerator");
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
        //AOLPoissonGenerator generator = new AOLPoissonGenerator();
        AOLPoissonGenerator generator = new AOLPoissonGenerator(args[0], args[1], args[2]);
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
