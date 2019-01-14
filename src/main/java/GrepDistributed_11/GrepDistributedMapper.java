package GrepDistributed_11;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// public class NgramsMapper extends Mapper {
public class GrepDistributedMapper extends Mapper <LongWritable, Text, Text, Text> {
    String grepedWord = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        grepedWord = context.getConfiguration().get("greped-word", "");
    }

    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] splits = value.toString().split("\\W+");
            List<String> words = Arrays.asList(splits);

            if (words.contains(grepedWord))
                context.write(new Text(grepedWord), value);
        }
    }
}
