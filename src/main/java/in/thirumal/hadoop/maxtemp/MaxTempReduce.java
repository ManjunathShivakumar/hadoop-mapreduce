package in.thirumal.hadoop.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by thirumal on 18-01-2015.
 */
public class MaxTempReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxVal = Integer.MIN_VALUE;
        // figure out the maximum value of the lot
        for (IntWritable v : values) {
            maxVal = Math.max(maxVal, v.get());
        }
        // finally write the maximum value for this particular key
        context.write(key, new IntWritable(maxVal));
    }
}
