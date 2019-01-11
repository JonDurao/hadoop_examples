package WordCounterCombinerCounter_4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// public class NgramsMapper extends Mapper {
public class WordCounterCombinerCounterMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] splits = value.toString().split("\\W+");

            for (String split : splits) {
                if (split.isEmpty())
                    context.getCounter(WordCounterCombinerCounterCounters.EMPTY_LINES).increment(1);
                else
                    context.write(new Text(split), new LongWritable(1));
            }
        }
    }
}
