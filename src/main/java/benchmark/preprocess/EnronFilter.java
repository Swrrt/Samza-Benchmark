package benchmark.preprocess;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class EnronFilter implements StreamTask{
    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector, TaskCoordinator taskCoordinator) throws Exception {
        String message = ((String)incomingMessageEnvelope.getMessage()).replaceAll("[^\\x00-\\x7F]", ""); //Remove non ASCII characters
        String filtered_email = "";
        for(String line :message.split("\n")){
            if(line.substring(0,5).equals("From:")){
                if(!line.substring(line.length()-10, line.length()-1).equals("@enron.com")){
                    // Not originate within Enron, drop it.
                    return ;
                }
            }else if(line.substring(0,3).equals("To:")){
                for(String address : line.substring(3).split(","))
                    //TODO
                    if(!line.substring(line.length()-10, line.length()-1).equals("@enron.com")){
                        // Not originate within Enron, drop it.
                        return ;
                    }

            }
        }
    }
}
