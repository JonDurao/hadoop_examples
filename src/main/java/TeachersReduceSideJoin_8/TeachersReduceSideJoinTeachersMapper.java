package TeachersReduceSideJoin_8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// public class WordCounterCombinerCounterMapper extends Mapper {
public class TeachersReduceSideJoinTeachersMapper extends Mapper <LongWritable, Text, TeachersReduceSideJoinWritable, Text> {
    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] splits = value.toString().split("\\W+");

            TeachersReduceSideJoinWritable trsjw = new TeachersReduceSideJoinWritable();
            trsjw.setDatasetId(TeachersReduceSideJoinWritable.TCHRS_DATASET);
            trsjw.setDptoId(new LongWritable(Long.parseLong(splits[1])));

            context.write(trsjw, new Text(splits[0] + "-" + splits[2] + "-" +  splits[3]));
        }
    }
}
