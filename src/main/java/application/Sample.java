package application;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;

import java.util.Random;

public class Sample implements StreamApplication{
    private static final String INPUT_TOPIC = "StreamBenchInput";
    private static final String OUTPUT_TOPIC = "SampleOutput";

    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
        // OutputStream<KV<String, WordCount>> outputStream = graph.getOutputStream(OUTPUT_TOPIC, KVSerde.of(new StringSerde(), new JsonSerdeV2<>(WordCount.class)));
        OutputStream<KV<String, String>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Sample the input with probability
        inputStream
                .filter((message) -> {
                    return (new Random()).nextInt(100)<50;
                })
                .sendTo(outputStream);
    }
}
