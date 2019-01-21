package application;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;

import java.time.Duration;

// test
public class window implements StreamApplication {

  // private static final Logger LOG = LoggerFactory.getLogger(TumblingPageViewCounterApp.class);
  //private static final String INPUT_TOPIC = "filterStage";
  private static final String INPUT_TOPIC = "WordCount";
  private static final String OUTPUT_TOPIC = "windowStage";

  @Override
  public void init(StreamGraph graph, Config config) {
    graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));

    MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
    OutputStream<KV<String, WordCount>> outputStream = graph.getOutputStream(OUTPUT_TOPIC, KVSerde.of(new StringSerde(), new JsonSerdeV2<>(WordCount.class)));

    inputStream
        .window(Windows.keyedTumblingWindow(
          kv -> kv.value, Duration.ofSeconds(3), () -> 0, (m, prevCount) -> prevCount + 1,
          new StringSerde(), new IntegerSerde()), "count")
        .map(windowPane -> {
          String word = windowPane.getKey().getKey();
          int count = windowPane.getMessage();
          return KV.of(word, new WordCount(word, count));
        })
        .sendTo(outputStream);
  }
}

