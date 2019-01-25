package benchmark.WordCount;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import java.util.Arrays;

public class WordSplitter implements StreamApplication{
    private static final String INPUT_TOPIC = "StreamBenchInput";
    private static final String OUTPUT_TOPIC = "WordSplitterOutput";

    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
        OutputStream<KV<String, String>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Split the input into multiple strings
        inputStream
                .flatMap((message) -> {
                    return Arrays.asList(message.getValue().split("|"));
                })
                .map((message) -> KV.of("", message))
                .sendTo(outputStream);
    }
}
