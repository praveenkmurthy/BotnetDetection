/**
 * Created by Keshavamurthy on 11/16/16.
 */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class IPSubnetMapper extends Mapper<LongWritable, Text, Text, NetFlowFormat> {

    private Logger logger = Logger.getLogger(FeatureMapper.class);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        NetFlowData flowData = NetFlowData.getInstance(value.toString(), context);
        if(flowData == null){
            return;
        }

        try {
            context.getCounter(flowData.getOutputLabel()).increment(1);
            context.getCounter(flowData.getDirection()).increment(1);
            context.write(new Text(flowData.getDestinationIPSubnet()), new NetFlowFormat(flowData));

        } catch (Exception e){
            logger.info("IPSubnetMapper failed with " + e.getMessage() + " " + Thread.currentThread().getStackTrace().toString());
            logger.error("Stack Trace", e);
        }

    }
}
