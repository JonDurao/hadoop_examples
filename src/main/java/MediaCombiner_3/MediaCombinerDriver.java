package MediaCombiner_3;

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

public class MediaCombinerDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ToolRunner.run(new MediaCombinerDriver(), args);
    }

    @Override
    public int run (String[] strings) throws Exception {
        final Job job = Job.getInstance(getConf());

        job.setMapperClass(MediaCombinerMapper.class);
        job.setCombinerClass(MediaCombinerCombiner.class);
        job.setReducerClass(MediaCombinerReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Seteamos los paths de entrada y salida del job (ARGS)
        // FileInputFormat.addInputPath(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("/home/bigdata/Workspace/hadoop_examples/src/main/resources/Media"));
        FileOutputFormat.setOutputPath(job, new Path("/home/bigdata/Workspace/hadoop_examples/src/main/resources/Media/mc_3_out"));

        job.setJarByClass(MediaCombinerDriver.class);

        // Lanzamos el job
        // submit no espera a que termine la ejecucion
        // job.submit();
        job.waitForCompletion(true);

        return 0;
    }
}
