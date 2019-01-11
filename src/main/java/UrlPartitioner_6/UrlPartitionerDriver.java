package UrlPartitioner_6;

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

public class UrlPartitionerDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ToolRunner.run(new UrlPartitionerDriver(), args);
    }

    @Override
    public int run (String[] strings) throws Exception {
        final Job job = Job.getInstance(getConf());

        job.setMapperClass(UrlPartitionerMapper.class);
        job.setReducerClass(UrlPartitionerReducer.class);
        job.setPartitionerClass(UrlPartitionerPartitioner.class);

        job.setNumReduceTasks(2);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Seteamos los paths de entrada y salida del job (ARGS)
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Urls"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Urls/up_6_out"));

        job.setJarByClass(UrlPartitionerDriver.class);

        // Lanzamos el job
        // submit no espera a que termine la ejecucion
        // job.submit();
        job.waitForCompletion(true);

        return 0;
    }
}
