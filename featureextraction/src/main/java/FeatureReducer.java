import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Keshavamurthy on 10/25/16.
 */
public class FeatureReducer extends Reducer<Text, NetFlowFormat, NullWritable, Text> {
    private Logger logger = Logger.getLogger(FeatureReducer.class);
    private HashMap<String, String> subnetFeatures = new HashMap<String, String>();

    private enum GroundTruth {
        NormalFlows, BotnetFlows, MultipleLabelFlows
    }

    private void updateCounters(Set<NetFlowData.label> label, Context context) {

        if(label.size() > 1){
            context.getCounter(GroundTruth.MultipleLabelFlows).increment(1);
        } else if(label.iterator().next().getValue() == 0){
            context.getCounter(GroundTruth.NormalFlows).increment(1);
        } else {
            context.getCounter(GroundTruth.BotnetFlows).increment(1);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = null;
        try {

            String sCurrentLine;
            String pathString = context.getConfiguration().get("fs.defaultFS") + "/" + context.getConfiguration().get("filePath");
            Path subnetFeaturesPath = new Path(pathString);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            br = new BufferedReader(new InputStreamReader(fs.open(subnetFeaturesPath)));

            while ((sCurrentLine = br.readLine()) != null) {
                String[] featureSet = sCurrentLine.split(" ", 2);
                subnetFeatures.put(featureSet[0], featureSet[1]);
            }

        } catch (IOException e) {
            logger.info("Reducer setup failed with " + e.getMessage() + " " + Thread.currentThread().getStackTrace().toString());
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

    }

    @Override
    protected void reduce(Text key, Iterable<NetFlowFormat> flowIterator, Context context)
            throws IOException, InterruptedException {

        DestinationIPFlowInfo flowStats = new DestinationIPFlowInfo(key.toString());

        Iterator<NetFlowFormat> valueIterator = flowIterator.iterator();
        while(valueIterator.hasNext()){
            NetFlowData flowData = valueIterator.next().toNetFlowData();
            flowStats.AddFlowToMap(flowData);
        }

        updateCounters(flowStats.getOutputLabel(), context);

        if(flowStats.getOutputLabel().size() == 1){
            try {
                context.write(NullWritable.get(), new Text(flowStats.getFeatureSet(this.subnetFeatures.get(flowStats.getDestinationIPSubnet()))));
            }
            catch (Exception e){
                logger.info("Reducer failed with " + e.getMessage() + " " + Thread.currentThread().getStackTrace().toString());
                logger.error("Stack Trace", e);
            }
        }

    }
}
