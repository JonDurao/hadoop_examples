package WordCounterCombiner_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// public class WordCounterCombinerMapper extends Mapper {
public class WordCounterCombinerMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] splits = value.toString().split("\\W+");

            for (String split : splits) {
                context.write(new Text(split), new LongWritable(1));
            }
        }
    }
}
