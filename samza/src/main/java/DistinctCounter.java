package benchmark;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;

import java.io.*;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

public class DistinctCounter implements StreamApplication {
    private static final String INPUT_TOPIC = "WordSplitterOutput";
    private static final String OUTPUT_TOPIC = "DistinctCounterOutput";

    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<KV<String, String>> inputStream = graph.getInputStream(INPUT_TOPIC);
        OutputStream<KV<String, String>> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Split the input into multiple strings
        inputStream
                .window(Windows.tumblingWindow(Duration.ofSeconds(10), Distinct::new, new distinctAggreggator(), Distinct.serde()), "distinctWindow")
                .map(windowPane -> {
                    return KV.of("", formatOutput((WindowPane<Void,Distinct>)windowPane));
                })
                .sendTo(outputStream);
    }

    public static class Distinct implements Serializable{
        public Set<String> distinct = new HashSet<>();
        @Override
        public String toString(){
            return distinct.toString();
        }
        static Serde<Distinct> serde(){
            return new DistinctSerdes();
        }
        public static class DistinctSerdes implements Serde<Distinct>{
            @Override
            public byte[] toBytes(Distinct obj){
                try{
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream dos = new ObjectOutputStream(bos);
                    dos.writeObject(obj.distinct);
                    return bos.toByteArray();
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
            @Override
            public Distinct fromBytes(byte[] bytes){
                try{
                    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                    ObjectInputStream ois = new ObjectInputStream(bis);
                    Distinct distinct = new Distinct();
                    distinct.distinct = (Set<String>)ois.readObject();
                    return distinct;
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            }
        }

    }
    private class distinctAggreggator implements FoldLeftFunction<KV<String, String>, Distinct>{
        @Override
        public void init(Config config, TaskContext taskContext){
        }
        @Override
        public Distinct apply(KV<String,String> string, Distinct distinct){
            if(!distinct.distinct.contains(string.getValue())){
                distinct.distinct.add(string.getValue());
            }
            return distinct;
        }
    }
    private String formatOutput(WindowPane<Void, Distinct> windowPane){
        Distinct distinct = windowPane.getMessage();
        return String.valueOf(distinct.distinct.size());
    }
}