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
        //Add a overall skewness using sin function.
        double skewnessPeriod = 30 * 1000000000l, upperBound = 2, lowerBound = 0.1;
        long lline = 0, line = 0, time = System.nanoTime(), ltime = time, interval = 1000000000l/speed /* Number of records per second*/, signal_interval = 3 * 1000000000l;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String curLine = br.readLine(); //Skip the first line of the file
            while ((curLine = br.readLine()) != null) {
                List<String> words = splitSentence(curLine);
                for(String word: words) {
                    if (isKeyPartition) processAOLformat(word, producer);
                    else processAOLformatWithKey(word, producer);
                    line++;
                    time = System.nanoTime();
                    if (time - ltime >= signal_interval) {
                        System.out.printf("word: %d, ", line - lline);
                        System.out.printf("time: %.3f\n\n", (time - ltime) / 1000000000.0);
                        ltime = time;
                        lline = line;
                    }
                    double curInterval =  interval * ((Math.cos(time / skewnessPeriod) + 1.6)/2); // 0.3~1.3 of original speed
                    //Gaussian distribution of short-term arrival
                    double stdDeviation = curInterval / 3;
                    double exponentialRandom = rand.nextGaussian() * stdDeviation + curInterval;
                    if(exponentialRandom > curInterval * 2) exponentialRandom = curInterval * 2;
                    if(exponentialRandom < curInterval * 0.5)exponentialRandom = curInterval * 0.5;
                    while (System.nanoTime() - time < exponentialRandom) ;         /* Control the throughput of producing*/
                    //if(line >= 10000)break;
                    //if (line >= numberToGenerate) break;
                }
            }
            producer.close();
            return line;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line;
    }
    private List<String> splitSentence(String sentence){
        String whitespace_chars =  ""       /* dummy empty string for homogeneity */
                + "\\u0009" // CHARACTER TABULATION
                + "\\u000A" // LINE FEED (LF)
                + "\\u000B" // LINE TABULATION
                + "\\u000C" // FORM FEED (FF)
                + "\\u000D" // CARRIAGE RETURN (CR)
                + "\\u0020" // SPACE
                + "\\u0085" // NEXT LINE (NEL)
                + "\\u00A0" // NO-BREAK SPACE
                + "\\u1680" // OGHAM SPACE MARK
                + "\\u180E" // MONGOLIAN VOWEL SEPARATOR
                + "\\u2000" // EN QUAD
                + "\\u2001" // EM QUAD
                + "\\u2002" // EN SPACE
                + "\\u2003" // EM SPACE
                + "\\u2004" // THREE-PER-EM SPACE
                + "\\u2005" // FOUR-PER-EM SPACE
                + "\\u2006" // SIX-PER-EM SPACE
                + "\\u2007" // FIGURE SPACE
                + "\\u2008" // PUNCTUATION SPACE
                + "\\u2009" // THIN SPACE
                + "\\u200A" // HAIR SPACE
                + "\\u2028" // LINE SEPARATOR
                + "\\u2029" // PARAGRAPH SEPARATOR
                + "\\u202F" // NARROW NO-BREAK SPACE
                + "\\u205F" // MEDIUM MATHEMATICAL SPACE
                + "\\u3000" // IDEOGRAPHIC SPACE
                + "\\s"
                + "\\"
                + "\\xA0"
                ;
        return new ArrayList<String>(Arrays.asList(sentence.split("[" + whitespace_chars + "]+")));
    }
    private long getPoissonRandom(double mean){
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
        ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, line, line);
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
