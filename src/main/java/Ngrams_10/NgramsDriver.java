package Ngrams_10;

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

public class NgramsDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ToolRunner.run(new NgramsDriver(), args);
    }

    @Override
    public int run (String[] strings) throws Exception {
        final Job job = Job.getInstance(getConf());

        job.getConfiguration().setInt("n-characters", 3);

        job.setMapperClass(NgramsMapper.class);
        job.setCombinerClass(NgramsCombiner.class);
        job.setReducerClass(NgramsReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Seteamos los paths de entrada y salida del job (ARGS)
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Ngrams"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Ngrams/ng_10_out"));

        job.setJarByClass(NgramsDriver.class);

        // Lanzamos el job
        // submit no espera a que termine la ejecucion
        // job.submit();
        job.waitForCompletion(true);

        System.out.println("EMPTY LINES: " + job.getCounters().findCounter(NgramsCounters.EMPTY_LINES).getValue());

        return 0;
    }
}
