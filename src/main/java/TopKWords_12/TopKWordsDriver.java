package TopKWords_12;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class TopKWordsDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ToolRunner.run(new TopKWordsDriver(), args);
    }

    @Override
    public int run (String[] strings) throws Exception {
        final String COMMON_PATH = "/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/TopKWords";
        Path temporal = new Path(COMMON_PATH + "_tmp");
        Path input = new Path(COMMON_PATH);
        Path output = new Path(COMMON_PATH + "/tkp_12_out");

        //------------- WORD COUNTER

        final Job jobWc = Job.getInstance(getConf());

        jobWc.setJarByClass(TopKWordsDriver.class);
        jobWc.setMapperClass(WordCounterMapper.class);
        jobWc.setReducerClass(WordCounterReducer.class);

        jobWc.setInputFormatClass(TextInputFormat.class);
        jobWc.setOutputFormatClass(TextOutputFormat.class);

        jobWc.setMapOutputKeyClass(Text.class);
        jobWc.setMapOutputValueClass(LongWritable.class);

        jobWc.setOutputKeyClass(Text.class);
        jobWc.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobWc, input);
        FileOutputFormat.setOutputPath(jobWc, temporal);

        jobWc.waitForCompletion(true);

        //------------- TOP K WORDS

        final Job jobtkw = Job.getInstance(getConf());

        jobtkw.setJarByClass(TopKWordsDriver.class);
        jobtkw.setMapperClass(TopKWordsMapper.class);
        jobtkw.setReducerClass(TopKWordsReducer.class);

        jobtkw.setInputFormatClass(TextInputFormat.class);
        jobtkw.setOutputFormatClass(TextOutputFormat.class);

        jobtkw.setMapOutputKeyClass(Text.class);
        jobtkw.setMapOutputValueClass(LongWritable.class);

        jobtkw.setOutputKeyClass(Text.class);
        jobtkw.setOutputValueClass(LongWritable.class);

        jobtkw.getConfiguration().setInt("topkwords.k", 3);

        FileInputFormat.addInputPath(jobtkw, new Path(COMMON_PATH + "_tmp"));
        FileOutputFormat.setOutputPath(jobtkw, output);

        jobtkw.waitForCompletion(true);

        return 0;
    }
}
