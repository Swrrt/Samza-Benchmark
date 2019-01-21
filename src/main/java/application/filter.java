package application;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.KVSerde;

// test
public class filter implements StreamApplication {

  // private static final Logger LOG = LoggerFactory.getLogger(TumblingPageViewCounterApp.class);
  private static final String INPUT_TOPIC = "WordCount";
  private static final String OUTPUT_TOPIC = "filterStage";

  @Override
  public void init(StreamGraph graph, Config config) {
    graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
    MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
    OutputStream<KV<String, String>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);

    inputStream
        .filter((kv) -> {
          String finalChar = kv.value.substring(kv.value.length()-1, kv.value.length());
          return !finalChar.equals("1");
        })
        .sendTo(outputStream);
  }
}

