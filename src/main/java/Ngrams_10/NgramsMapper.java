package Ngrams_10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// public class NgramsMapper extends Mapper {
public class NgramsMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
    private final LongWritable one = new LongWritable(1);
    private int n = 0;
    private List<StringBuilder> nGrams = new ArrayList<>();
    private Set<String> words = new HashSet<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        n = context.getConfiguration().getInt("n-characters", 2);
    }

    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] wordsLine = value.toString().toLowerCase().split("\\W+");

            for (String word : wordsLine) {
                if (word.isEmpty())
                    context.getCounter(NgramsCounters.EMPTY_LINES).increment(1);
                else
                    words.add(word);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String word : words) {
            buildNgram(word);
        }

        for (StringBuilder nGram : nGrams) {
            context.write(new Text(nGram.toString()), one);
        }

        super.cleanup(context);
    }

    private void buildNgram(String word) {
        if (!word.isEmpty()) {
            for (StringBuilder nGram : nGrams) {
                if (nGram.toString().split(" ").length < n)
                    nGram.append(" ").append(word);
            }

            nGrams.add(new StringBuilder(word));
        }
    }
}
