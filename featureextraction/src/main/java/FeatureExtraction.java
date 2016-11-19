import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Iterator;

/**
 * Created by Keshavamurthy on 10/25/16.
 */
public class FeatureExtraction {

    public enum CustomCounters {
        BADINPUT, HEADER, INVALIDPROTOCOL
    }

    private static void executeJob(Configuration conf, String inputPath, String outputPath, int noOfReducers,
                                   Class<? extends Mapper> map, Class<? extends Reducer> reduce) throws Exception {
        Job job = Job.getInstance(conf, "Feature extractor - Connection");
        job.setJarByClass(FeatureExtraction.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setNumReduceTasks(noOfReducers);
        job.setMapperClass(map);
        job.setReducerClass(reduce);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NetFlowFormat.class);
        if (!job.waitForCompletion(true)) {
            System.out.println("job 1 failed");
            System.exit(-1);
        }

        Counters counters = job.getCounters();
        for (CounterGroup group : counters) {
            System.out.println("* Counter Group: " + group.getDisplayName() + " (" + group.getName() + ")");
            System.out.println("  number of counters in this group: " + group.size());
            for (Counter counter : group) {
                System.out.println("  - " + counter.getDisplayName() + ": " + counter.getName() + ": "+counter.getValue());
            }
        }

    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: extractfeature <input_path> <ouput_path>");
            System.exit(1);

        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");

        FeatureExtraction.executeJob(conf, args[0], args[1] + "/Step1", 1, IPSubnetMapper.class, IPSubnetReducer.class);
        conf.set("filePath", args[1] + "/Step1/part-r-00000");
        FeatureExtraction.executeJob(conf, args[0], args[1] + "/Step2", 1, FeatureMapper.class, FeatureReducer.class);


    }
}
