package TeachersReduceSideJoin_8;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

public class TeachersReduceSideJoinDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        ToolRunner.run(new TeachersReduceSideJoinDriver(), args);
    }

    @Override
    public int run (String[] strings) throws Exception {
        final Job job = Job.getInstance(getConf());
        job.setJarByClass(TeachersReduceSideJoinDriver.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Teachers/InputTeachers.txt"), TextInputFormat.class, TeachersReduceSideJoinTeachersMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Teachers/InputTeachersDepartments.txt"), TextInputFormat.class, TeachersReduceSideJoinDepartmentsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("/Users/jondurao/Documents/Master/Exercices/Hadoop/hadoop_examples/src/main/resources/Teachers/trsj_8_out"));

        job.setMapOutputKeyClass(TeachersReduceSideJoinWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setGroupingComparatorClass(TeachersReduceSideJoinComparator.class);
        job.setReducerClass(TeachersReduceSideJoinReducer.class);
        job.setPartitionerClass(TeachersReduceSideJoinPartitioner.class);
        job.setNumReduceTasks(2);

        // Lanzamos el job
        // submit no espera a que termine la ejecucion
        // job.submit();
        job.waitForCompletion(true);

        return 0;
    }
}
