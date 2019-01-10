package MediaCombiner_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// public class WordCounterCombinerMapper extends Mapper {
public class MediaCombinerMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] splits = value.toString().split("\\W+");

            context.write(new Text(splits[0]), new LongWritable(Long.parseLong(splits[1])));
        }
    }
}
