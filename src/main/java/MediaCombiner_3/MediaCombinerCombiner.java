package MediaCombiner_3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MediaCombinerCombiner extends Reducer <Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce (Text key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {
        long count = 0, media = 0, total = 0;

        for (LongWritable value : values) {
            total+=Long.parseLong(value.toString());
            count++;
        }

        media = total / count;

        context.write(key, new LongWritable(media));
    }
}
