package MediaCombiner_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MediaCombinerReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce (Text key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {
        long finalValue = 0;

        for (LongWritable value : values) {
            finalValue = Long.parseLong(value.toString());
        }

        context.write(key, new LongWritable(finalValue));
    }
}
