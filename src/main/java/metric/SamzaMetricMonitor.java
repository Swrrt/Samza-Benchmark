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
        while(true){
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
    }
    private void parseProcessEnvelopes(String record){
        JSONObject json = new JSONObject(record);
        //System.out.println(json);
        if(json.getJSONObject("header").getString("job-name").equals(appName) && json.getJSONObject("header").getString("container-name").contains("samza-container")){
            //System.out.println("!!!!!!\n"+json.getJSONObject("metrics")+"!!!!!!\n");
            if(json.getJSONObject("metrics").has("org.apache.samza.container.SamzaContainerMetrics")) {
                long processEnvelopes = json.getJSONObject("metrics").
                        getJSONObject("org.apache.samza.container.SamzaContainerMetrics").
                        getLong("process-envelopes");
                double processLatency = json.getJSONObject("metrics").
                        getJSONObject("org.apache.samza.container.SamzaContainerMetrics").
                        getLong("process-ns");
                double windowLatency = json.getJSONObject("metrics").
                        getJSONObject("org.apache.samza.container.SamzaContainerMetrics").
                        getLong("window-ns");
                double commitLatency = json.getJSONObject("metrics").
                        getJSONObject("org.apache.samza.container.SamzaContainerMetrics").
                        getLong("commit-ns");
                double totalLatency = processLatency + windowLatency + commitLatency;
                long time = json.getJSONObject("header").getLong("time");
                String containerId = json.getJSONObject("header").getString("container-name");
                long dEnv = processEnvelopes;
                if (processEnv.containsKey(containerId)) {
                    if(processEnvelopes > processEnv.get(containerId)) /* When container restart (due to scaling or load rebalancing), the processed envelopes will start from 0 again */
                        dEnv -= processEnv.get(containerId);
                }
                long dTime = time;
                if (processTime.containsKey(containerId)) {
                    dTime -= processTime.get(containerId);
                    double throughput = 0;
                    if (dTime > 0) throughput = dEnv / ((double) dTime);
                    if (avgThroughput.containsKey(containerId)) {
                        totalThroughput -= avgThroughput.get(containerId);
                    }
                    avgThroughput.put(containerId, throughput);
                    totalThroughput += throughput;
                    //System.out.printf("%.2f Container ID: %s, throughput: %.2f, ", time / 1000.0, containerId, throughput * 1000);
                    //System.out.printf("total throughput: %.2f\n", totalThroughput * 1000);
                    
                    System.out.printf("%.2f %s %.2f %.2f %.2f\n", time / 1000.0, containerId, throughput * 1000, totalLatency / 1000000.0, totalThroughput * 1000);

                }
                processEnv.put(containerId, processEnvelopes);
                processTime.put(containerId, time);
            }
        }
    }

    /*
     *
     * @param brokers brokers
     * @param groupId
     * @return
     */
    private static Properties createConsumerConfig(String brokers, String appName) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SamzaMetricsReader");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //props.put("auto.commit.enable", "false");

        return props;
    }

    public static void main(String[] args) throws InterruptedException {
        String brokers = args[0];
        // String groupId = args[1];
        String topic = args[1];
        String appName = args[2];
        Properties props = createConsumerConfig(brokers, appName);
        SamzaMetricMonitor example = new SamzaMetricMonitor(props, topic);
        example.appName = args[2];
        example.run();

        example.shutdown();
    }
}