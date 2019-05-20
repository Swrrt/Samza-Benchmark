package generator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import org.apache.log4j.Logger;

/**
 * SSE generaor
 */
class SSEGnerator {

    private String TOPIC;

    private static KafkaProducer<String, String> producer;

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;
    private static final int Last_Upd_Time = 3;
    public SSEGnerator(String input, String bootstrapServer) {
        TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);

    }

    public void generate(String file, int speed, long startPoint) throws InterruptedException {

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null;
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        long interval = 0;
        int counter = 0;
        try {
            stream = new FileReader(file);
            br = new BufferedReader(stream);
//            Thread.sleep(10000);
            while ((sCurrentLine = br.readLine()) != null) {
                long time = 0;
                if(sCurrentLine.split("\\|").length >= 10) {
                    String t = sCurrentLine.split("\\|")[Last_Upd_Time];
                    System.out.println("Last upd time:!!!" + t);
                    time = Duration.between(LocalTime.MIN, LocalTime.parse(t)).toMillis() / 1000;
                }
                if(time > startPoint) {
                    if (sCurrentLine.equals("end")) {
                        counter++;
                    }
                    if (counter == speed) {
                        start = System.nanoTime();
                        interval = 1000000000 / textList.size();
                        for (int i = 0; i < textList.size(); i++) {
                            cur = System.nanoTime();
                            if (textList.get(i).split("\\|").length < 10) {
                                continue;
                            }
                            ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, textList.get(i).split("\\|")[Sec_Code], textList.get(i));
                            producer.send(newRecord);
                            while ((System.nanoTime() - cur) < interval) {
                            }
                        }
                        //}
                        System.out.println("size:" + String.valueOf(textList.size()));
                        System.out.println("time:" + String.valueOf((System.nanoTime() - start) / 1000000));
                        System.out.println("interval:" + String.valueOf((System.nanoTime() - start) / (textList.size())));
                        textList.clear();
                        counter = 0;
                        continue;
                    }
                    textList.add(sCurrentLine);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }
        producer.close();
        //logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    public static void main(String[] args) throws InterruptedException {
        String TOPIC = new String("stock");
        String file = new String("partition1");
        int speed = 1;
        long startPoint = 0;  //In second
        String bootstrapServer = "localhost:9092";
        if (args.length > 0) {
            TOPIC = args[0];
            file = args[1];
            speed = Integer.parseInt(args[2]);
            startPoint = Long.parseLong(args[3]);
            bootstrapServer = args[4];
        }
        new SSEGnerator(TOPIC, bootstrapServer).generate(file, speed, startPoint);
    }
}
