package application.WordCount;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStorageEngine;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;

import java.time.Duration;
import java.util.Arrays;

public class WordCounter implements StreamApplication{
    private static final String INPUT_TOPIC = "WordSplitterOutput";
    private static final String OUTPUT_TOPIC = "WordCounterOutput";
    private KeyValueStore<String, Integer> counter;
    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<String> inputStream = graph.getInputStream(INPUT_TOPIC);
        OutputStream<KV<String, Integer>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Split the input into multiple strings
        inputStream
                .window(Windows.keyedTumblingWindow(
                        string -> string, Duration.ofDays(1), () -> 0, (m, prevCount) -> prevCount + 1,
                        new StringSerde(), new IntegerSerde()), "count")
                .map(windowPane -> {
                    String word = windowPane.getKey().getKey();
                    int count = windowPane.getMessage();
                    return KV.of(word, count);
                })
                .sendTo(outputStream);
    }

}
