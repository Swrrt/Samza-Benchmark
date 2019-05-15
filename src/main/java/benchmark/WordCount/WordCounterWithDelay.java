package benchmark.WordCount;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;

import java.time.Duration;
import java.util.Random;

public class WordCounterWithDelay implements StreamApplication{
    private static final String INPUT_TOPIC = "WordSplitterOutput";
    private static final String OUTPUT_TOPIC = "WordCounterOutput";
    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
        OutputStream<KV<String, String>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Split the input into multiple strings
        inputStream
                .window(Windows.keyedTumblingWindow(
                        message -> message.getValue(), Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
                        new StringSerde(), new IntegerSerde()), "count")
                .map(windowPane -> {
                    String word = windowPane.getKey().getKey();
                    long count = windowPane.getMessage();

                    //Add delay to each message
                    Random rand = new Random();
                    long mean = 4 * 10000000l, delayTimes = (long)((rand.nextGaussian() + 0.5) * mean) + mean;
                    for(long i=0;i<delayTimes;i++) {
                        count+=i;
                    }
                    for(long i=0;i<delayTimes;i++) {
                        count-=i;
                    }

                    return KV.of(word, String.valueOf(count));
                })
                .sendTo(outputStream);
    }

}
