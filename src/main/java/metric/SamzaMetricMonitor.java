package metric;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.*;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SamzaMetricMonitor {
    private final KafkaConsumer<String,String> consumer;
    private final String topic;
    private ExecutorService executor;
    private long delay;


    public SamzaMetricMonitor(Properties props, String topic) {
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
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
                        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
                    }
                }catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        });
    }
    private String parseJson(String record){
        JSONObject json = new JSONObject(record);
        if(json.getJSONObject("header").getString("container-name").contains("samza-container")){
            int processEnvelopes = json.getJSONObject("metrics").getJSONObject("org.apache.samza.container.SamzaContainerMetrics").getInt("process-envelopes");
        }
        return "";
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
        String brokers = args[0];
        // String groupId = args[1];
        String topic = args[1];
        Properties props = createConsumerConfig(brokers);
        SamzaMetricMonitor example = new SamzaMetricMonitor(props, topic);
        example.run();

        Thread.sleep(60*1000);

        example.shutdown();
    }
}