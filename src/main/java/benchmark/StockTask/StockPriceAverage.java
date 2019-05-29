package benchmark.StockTask;

import joptsimple.OptionSet;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.util.CommandLine;

import java.util.HashMap;
import java.util.Map;

public class StockPriceAverage implements StreamApplication {

    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;
    private static final long LoopsForOneMilliSecond = 100000l;
    private static final long DefaultDelay = 50; //ms
    Map<String, Float> stockAvgPriceMap = new HashMap<String, Float>();

    private static final String INPUT_TOPIC = "WordSplitterOutput";
    private static final String OUTPUT_TOPIC = "WordCounterOutput";
    private KeyValueStore<String, Integer> counter;
    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
        OutputStream<KV<String, String>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Split the input into multiple strings
        inputStream
                .map(order -> {
                    String[] orderArr = order.getValue().split("\\|");
                    //Add fixed delay
                    long t = 0;
                    for(long i = 0; i < config.getLong("job.delay.time.ms", DefaultDelay) * LoopsForOneMilliSecond; i++){
                        t += i;
                    }
                    if(t > 1) {
                        return new KV(order.getKey(), orderArr);
                    }else return new KV(order.getKey(), orderArr[1]);
                })
                .map((KV m) -> computeAverage(m))
                .map(orderSum -> {
                    long latency = System.nanoTime() - Long.valueOf(orderSum.getKey().split("\\|")[1]);
                    System.out.println(latency/1000);
                    return orderSum;
                })
                .sendTo(outputStream);
    }

    private KV<String, String> computeAverage(KV m) {
        // add payload here, if the process is not powerful enough
        // cur = System.nanoTime();
        // while ((System.nanoTime() - cur) < 1000) {}
        String[] orderArr = (String[]) m.getValue();
        if (!stockAvgPriceMap.containsKey(orderArr[Sec_Code])) {
            stockAvgPriceMap.put(orderArr[Sec_Code], (float) 0);
        }
        float sum = stockAvgPriceMap.get(orderArr[Sec_Code]) + Float.parseFloat(orderArr[Order_Price]);
        stockAvgPriceMap.put(orderArr[Sec_Code], sum);
        return new KV(m.getKey(), String.valueOf(sum));
    }

//    public static void main(String[] args) {
//        CommandLine cmdLine = new CommandLine();
//        OptionSet options = cmdLine.parser().parse(args);
//        Config config = cmdLine.loadConfig(options);
//        LocalApplicationRunner runner = new LocalApplicationRunner(config);
//        runner.run(new StockPriceAverage());
//        runner.waitForFinish();
//    }
}
