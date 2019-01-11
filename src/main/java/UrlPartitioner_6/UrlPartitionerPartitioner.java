package UrlPartitioner_6;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.net.MalformedURLException;
import java.net.URL;

public class UrlPartitionerPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text key, LongWritable value, int reduces) {
        try {
            URL url = new URL(key.toString());
            return Math.abs(url.getHost().hashCode()) % reduces;
        } catch (MalformedURLException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
