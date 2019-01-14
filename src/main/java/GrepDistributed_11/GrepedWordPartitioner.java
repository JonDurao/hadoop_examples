package GrepDistributed_11;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.net.MalformedURLException;
import java.net.URL;

public class GrepedWordPartitioner extends Partitioner<Text, Text> {
    int count = 0;

    @Override
    public int getPartition(Text key, Text value, int reduces) {
        count ++;
        return Math.abs(count) % reduces;
    }
}
