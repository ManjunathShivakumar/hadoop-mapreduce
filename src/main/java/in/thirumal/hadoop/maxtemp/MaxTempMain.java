package in.thirumal.hadoop.maxtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by thirumal on 18-01-2015.
 *
 * Get the data from here: https://github.com/tomwhite/hadoop-book/tree/master/input/ncdc/all
 */
public class MaxTempMain {
    public static void main(String [] args)
            throws IOException, InterruptedException, ClassNotFoundException {
        // Initiate the map reduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max temperature");
        job.setJarByClass(MaxTempMain.class);
        job.setMapperClass(MaxTempMap.class);
        job.setReducerClass(MaxTempReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
