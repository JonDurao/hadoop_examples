package WordCounterCombinerCounter_4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCounterCombinerCounterReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce (Text key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {
        long count = 0;

        for (LongWritable value : values) {
            if (Long.parseLong(value.toString()) > 1)
                count = Long.parseLong(value.toString());
            else
                count++;
        }

        context.write(key, new LongWritable(count));
    }
}
