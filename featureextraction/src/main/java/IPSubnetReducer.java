import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Keshavamurthy on 11/16/16.
 */
public class IPSubnetReducer extends Reducer<Text, NetFlowFormat, Text, Text> {

    private Logger logger = Logger.getLogger(FeatureReducer.class);

    @Override
    protected void reduce(Text key, Iterable<NetFlowFormat> flowIterator, Context context)
            throws IOException, InterruptedException {

        int totalFlows = 0, totalPackets = 0;
        HashSet<String> distinctDestinationIPs = new HashSet<String>();

        Iterator<NetFlowFormat> valueIterator = flowIterator.iterator();
        while(valueIterator.hasNext()){
            NetFlowData flowData = valueIterator.next().toNetFlowData();
            distinctDestinationIPs.add(flowData.getDestinationIP());
            totalFlows++;
            totalPackets += flowData.getTotalPackets();
        }

        String outputFeatures = String.format("%s %s %s",
                Integer.toString(distinctDestinationIPs.size()),
                Integer.toString(totalFlows), Integer.toString(totalPackets));

        try {
            context.write(key, new Text(outputFeatures));
        }
        catch (Exception e){
            logger.info("IPSubnetReducer failed with " + e.getMessage() + " " + Thread.currentThread().getStackTrace().toString());
            logger.error("Stack Trace", e);
        }
    }
}
