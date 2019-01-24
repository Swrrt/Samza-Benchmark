package application;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.KeyValueStorageEngine;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.apache.samza.task.TaskContext;

import java.awt.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DistinctCounter implements StreamApplication {
    private static final String INPUT_TOPIC = "WordSplitterOutput";
    private static final String OUTPUT_TOPIC = "DistinctCounterOutput";
    private KeyValueStore<String, Integer> counter;

    @Override
    public void init(StreamGraph graph, Config config) {
        graph.setDefaultSerde(KVSerde.of(new StringSerde(), new StringSerde()));
        MessageStream<String> inputStream = graph.getInputStream(INPUT_TOPIC);
        OutputStream<String> outputStream = graph.getOutputStream(OUTPUT_TOPIC);
        // Split the input into multiple strings
        inputStream
                .window(Windows.tumblingWindow(Duration.ofSeconds(10), Distinct::new, new distinctAggreggator(), new DistinctSerdes()), "distinctWindow")
                .map(windowPane -> {
                    return formatOutput((WindowPane<Void,Distinct>)windowPane);
                })
                .sendTo(outputStream);
    }

    private static class Distinct {
        Set<String> distinct = new HashSet<>();
        private Distinct(){
        }
        private Distinct(HashSet<String> a){
            distinct.addAll(a);
        }
        @Override
        public String toString(){
            return distinct.toString();
        }
    }
    private class distinctAggreggator implements FoldLeftFunction<String, Distinct>{
        @Override
        public void init(Config config, TaskContext taskContext){
        }

        public Distinct apply(String string, Distinct distinct){
            if(!distinct.distinct.contains(string)){
                distinct.distinct.add(string);
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