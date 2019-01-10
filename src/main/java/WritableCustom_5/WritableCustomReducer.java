package WritableCustom_5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WritableCustomReducer extends Reducer <Text, WritableCustomWritable, Text, DoubleWritable> {
    @Override
    protected void reduce (Text key, Iterable<WritableCustomWritable> values, Context context) throws IOException,
            InterruptedException {
        DoubleWritable total = new DoubleWritable(0);

        for (WritableCustomWritable value : values) {
            total.set(value.getFirst().get() + value.getSecond().get());
        }

        context.write(key, total);
    }
}
