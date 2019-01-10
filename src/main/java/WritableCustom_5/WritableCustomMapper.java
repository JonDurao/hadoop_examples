package WritableCustom_5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// public class WordCounterCombinerCounterMapper extends Mapper {
public class WritableCustomMapper extends Mapper <LongWritable, Text, Text, WritableCustomWritable> {
    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] splits = value.toString().split("\\W+");

            context.write(new Text(splits[0]), new WritableCustomWritable(Double.parseDouble(splits[1]), Double.parseDouble(splits[2])));
        }
    }
}
