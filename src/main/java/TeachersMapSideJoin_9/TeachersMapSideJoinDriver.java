package TeachersMapSideJoin_9;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.net.URI;

public class TeachersMapSideJoinDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ToolRunner.run(new TeachersMapSideJoinDriver(), args);
    }

    @Override
    public int run (String[] strings) throws Exception {
        final Job job = Job.getInstance(getConf());
        job.setJarByClass(TeachersMapSideJoinDriver.class);

        job.addCacheFile(new URI("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Teachers/InputTeachersDepartments.txt"));
        FileInputFormat.addInputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Teachers/9"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Teachers/9/tmsj_9_out"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TeachersMapSideJoinMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        // Asi la salida la genera el MAPPER
        job.setNumReduceTasks(0);

        /*job.setMapOutputKeyClass(TeachersMapSideJoinWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(TeachersMapSideJoinComparator.class);
        job.setReducerClass(TeachersMapSideJoinReducer.class);
        job.setPartitionerClass(TeachersMapSideJoinPartitioner.class);
        job.setNumReduceTasks(2);*/

        // Lanzamos el job
        // submit no espera a que termine la ejecucion
        // job.submit();
        job.waitForCompletion(true);

        return 0;
    }
}
