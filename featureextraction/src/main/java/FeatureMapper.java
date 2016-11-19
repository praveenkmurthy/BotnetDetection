import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Keshavamurthy on 10/25/16.
 */
public class FeatureMapper extends Mapper<LongWritable, Text, Text, NetFlowFormat> {
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
            context.write(new Text(flowData.getDestinationIP()), new NetFlowFormat(flowData));

        } catch (Exception e){
            logger.info("Mapper failed with " + e.getMessage() + " " + Thread.currentThread().getStackTrace().toString());
            logger.error("Stack Trace", e);
        }
    }
}
