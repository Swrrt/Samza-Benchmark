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
                .window(Windows.tumblingWindow(Duration.ofSeconds(10), Distinct::new, new distinctAggreggator(), new DistinctSerdes()), "distinctWindow")
                .map(windowPane -> {
                    return KV.of("", formatOutput((WindowPane<Void,Distinct>)windowPane));
                })
                .sendTo(outputStream);
    }

    private static class Distinct implements Serializable{
        public HashSet<String> distinct = new HashSet<>();
        private Distinct(){
        }
        private Distinct(HashSet<String> a){
            distinct = new HashSet<>();
            distinct.addAll(a);
        }
        @Override
        public String toString(){
            return distinct.toString();
        }
    }
    private class distinctAggreggator implements FoldLeftFunction<KV<String, String>, Distinct>{
        @Override
        public void init(Config config, TaskContext taskContext){
        }

        public Distinct apply(KV<String,String> string, Distinct distinct){
            if(!distinct.distinct.contains(string.getValue())){
                distinct.distinct.add(string.getValue());
            }
            return distinct;
        }
    }
    private class DistinctSerdes implements Serde<Distinct>{
        private final String encoding;
        public DistinctSerdes(){
            encoding = "UTF-8";
        }
        public byte[] toBytes(Distinct obj){
            if(obj != null){
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = null;
                byte[] bytes = null;
                try{
                    out = new ObjectOutputStream(bos);
                    out.writeObject(obj.distinct);
                    out.flush();
                    bytes = bos.toByteArray();

                }catch (Exception e){
                }
                try{
                    bos.close();
                }catch (IOException e){
                }
                return bytes;
            }else{
                return null;
            }
        }
        public Distinct fromBytes(byte[] bytes){
            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
            ObjectInput in = null;
            Distinct distinct = null;
            try{
                in = new ObjectInputStream(bis);
                distinct = new Distinct((HashSet<String>)in.readObject());
            }catch (Exception e){
            }
            try{
                if(in!=null)in.close();
            }catch (IOException ex){
            }
            return distinct;
        }
    }
    private String formatOutput(WindowPane<Void, Distinct> windowPane){
        Distinct distinct = windowPane.getMessage();
        return String.valueOf(distinct.distinct.size());
    }
}