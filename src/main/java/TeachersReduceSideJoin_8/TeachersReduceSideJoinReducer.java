package TeachersReduceSideJoin_8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TeachersReduceSideJoinReducer extends Reducer <TeachersReduceSideJoinWritable, Text, Text, NullWritable> {
    @Override
    protected void reduce(TeachersReduceSideJoinWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean first = true;
        String dptoName = "";

        for(Text value: values) {
            if (first) {
                first = false;
                dptoName = value.toString();
            } else {
                context.write(new Text(value + "-" + dptoName), null);
            }
        }
    }
}
