package Ngrams_10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

// public class NgramsMapper extends Mapper {
public class NgramsMapper extends Mapper <LongWritable, Text, Text, LongWritable> {
    private final LongWritable one = new LongWritable(1);
    private Text outKey = new Text();
    private StringBuilder sb = new StringBuilder();
    private int n = 0;
    private TreeSet<String> nGrams = new TreeSet<>();

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
                nGrams.add(word);

                if (nGrams.size() == n) {
                    outKey.set(buildNGram(nGrams));
                    nGrams.pollFirst();
                    context.write(outKey, new LongWritable(1));
                }
            }
        }
    }

    /*@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String word : words) {
            buildNgram(word);
        }

        for (StringBuilder nGram : nGrams) {
            context.write(new Text(nGram.toString()), one);
        }

        super.cleanup(context);
    }*/

    private String buildNGram(Set<String> ngrams) {
        sb.setLength(0);
        for (String ngram : ngrams) {
            sb.append(ngram).append(" ");
        }
        return sb.toString().trim();
    }

    /*private void buildNgram(String word) {

        if (!word.isEmpty()) {
            for (StringBuilder nGram : nGrams) {
                if (nGram.toString().split(" ").length < n)
                    nGram.append(" ").append(word);
            }

            nGrams.add(new StringBuilder(word));
        }
    }*/
}
