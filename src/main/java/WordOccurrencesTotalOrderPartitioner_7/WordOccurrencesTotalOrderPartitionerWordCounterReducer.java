package WordOccurrencesTotalOrderPartitioner_7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordOccurrencesTotalOrderPartitionerWordCounterReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce (Text key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {
        context.write(key, values.iterator().next());
    }
}
