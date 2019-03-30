package benchmark.WordCount;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;

import java.util.ArrayList;
import java.util.Arrays;

public class WordSplitterWithLimits implements StreamApplication{
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
                    //return Arrays.asList(message.getValue().split("\\|"));
                    // TODO: change the split
                    // The split could cause memory error
                    //return Arrays.asList(message.getValue().split("[\\s\\xA0]+"));
                    ArrayList<String> result = new ArrayList();
                    for(int i=0; i < message.getValue().split("[\\\\s\\\\xA0]+").length && i < config.getInt("job.wordsplitter.limit");i++){
                        result.add(new String(message.getValue().split("[\\\\s\\\\xA0]+")[i]));
                    }
                    return result;
                })
                .map((message) -> KV.of(message, message))
                .sendTo(outputStream);
    }
}
