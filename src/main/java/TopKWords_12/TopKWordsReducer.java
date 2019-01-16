package TopKWords_12;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TopKWordsReducer extends Reducer <Text, LongWritable, Text, LongWritable> {
    private int k = 0;
    private Map<String, Long> words = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = context.getConfiguration().getInt("topkwords.k", k);
    }

    @Override
    protected void reduce (Text key, Iterable<LongWritable> values, Context context) throws IOException,
            InterruptedException {
        /*context.write(key, values.iterator().next());*/

        long x = values.iterator().next().get();

        if (x > k)
            words.put(key.toString(), x);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        words = words.entrySet().stream().sorted(Map.Entry.comparingByValue()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        words.entrySet().stream().forEach(v -> {
            try {
                context.write(new Text(v.getKey()), new LongWritable(v.getValue()));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }
}
