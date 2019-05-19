package benchmark.WordCount;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;

import java.util.Random;

public class StatelessWithDelay implements StreamApplication{
    private static final String INPUT_TOPIC = "WordSplitterOutput";
    private static final String OUTPUT_TOPIC = "WordCounterOutput";
    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
        OutputStream<KV<String, String>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Split the input into multiple strings
        inputStream
                .map(a -> {
                    String word = a.getKey();
                    long count = a.getValue().length();

                    //Add delay to each message
                    Random rand = new Random();
                    // 10000000: 100 mps
                    long mean = 1 * 10000000l, delayTimes = (long)(rand.nextGaussian() * mean) + mean;
                    if(delayTimes > mean * 1.5) delayTimes = (long)(mean * 1.5);
                    if(delayTimes < mean * 0.5) delayTimes = (long)(mean * 0.5);
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
