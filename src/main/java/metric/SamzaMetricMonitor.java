package metric;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SamzaMetricMonitor {
    private final KafkaConsumer<String,String> consumer;
    private final String topic;
    private ExecutorService executor;
    private long delay;
    private String appName;
    private HashMap<String, Long> processEnv;
    private HashMap<String, Long> processTime;
    private HashMap<String, Double> avgThroughput;
    private double totalThroughput;
    public SamzaMetricMonitor(Properties props, String topic) {
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        processEnv = new HashMap<>();
        processTime = new HashMap<>();
        avgThroughput = new HashMap<>();
        totalThroughput = 0;
    }

    public void shutdown() {
        if (consumer != null)
            consumer.close();
        if (executor != null)
            executor.shutdown();
    }

    public void run() {
        consumer.subscribe(Collections.singletonList(this.topic));

        Executors.newSingleThreadExecutor().execute(() -> {
            while(true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    for (ConsumerRecord<String, String> record : records) {
                        // sent kafka msg by http
                        // System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                        parseProcessEnvelopes(record.value());
                    }
                }catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
    }
    private void parseProcessEnvelopes(String record){
        JSONObject json = new JSONObject(record);
        if(json.getJSONObject("header").getString("container-name").contains("samza-container")){
            long processEnvelopes = json.getJSONObject("metrics").getJSONObject("org.apache.samza.container.SamzaContainerMetrics").getLong("process-envelopes");
            long time = json.getJSONObject("header").getLong("time");
            String containerId = json.getJSONObject("header").getString("container-name");
            long dEnv = processEnvelopes;
            if(processEnv.containsKey(containerId)) {
                dEnv -= processEnv.get(containerId);
            }
            long dTime = time;
            if(processTime.containsKey(containerId)) {
                dTime -= processTime.get(containerId);
                double throughput = dEnv / ((double) dTime);
                if(avgThroughput.containsKey(containerId)){
                    totalThroughput -= avgThroughput.get(containerId);
                }
                avgThroughput.put(containerId, throughput);
                totalThroughput += throughput;
                System.out.printf("%d Current total throughput: %f\n", time, totalThroughput);
            }
            processEnv.put(containerId, processEnvelopes);
            processTime.put(containerId, time);
        }
    }

    /*
     *
     * @param brokers brokers
     * @param groupId
     * @return
     */
    private static Properties createConsumerConfig(String brokers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //props.put("auto.commit.enable", "false");

        return props;
    }

    public static void main(String[] args) throws InterruptedException {
        String brokers = args[1];
        // String groupId = args[1];
        String topic = args[0];
        Properties props = createConsumerConfig(brokers);
        SamzaMetricMonitor example = new SamzaMetricMonitor(props, topic);
        //example.appName = args[2];
        example.run();

        Thread.sleep(60*1000);

        example.shutdown();
    }
}