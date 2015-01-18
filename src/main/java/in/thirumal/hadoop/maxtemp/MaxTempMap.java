package in.thirumal.hadoop.maxtemp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by thirumal on 18-01-2015.
 */
public class MaxTempMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // convert the mapper value to java string
        String line = value.toString();
        // fetch the year from the line
        String year = line.substring(15, 19);
        int airTemp;
        // parse the temperature in the string
        // parseInt does not like the preceding + sign
        if(line.charAt(87) == '+') {
            airTemp = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemp = Integer.parseInt(line.substring(87, 92));
        }
        // See if the quality of this particular line is good or not
        String quality = line.substring(92, 93);
        // if all things are good, go ahead and emit the values to the framework
        if(airTemp != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemp));
        }
    }
}
