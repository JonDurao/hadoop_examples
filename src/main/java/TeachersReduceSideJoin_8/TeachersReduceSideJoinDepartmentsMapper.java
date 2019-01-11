package TeachersReduceSideJoin_8;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// public class NgramsMapper extends Mapper {
public class TeachersReduceSideJoinDepartmentsMapper extends Mapper <LongWritable, Text, TeachersReduceSideJoinWritable, Text> {
    @Override
    // protected void map (Object key, Object value, Context context) throws IOException, InterruptedException {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            String[] splits = value.toString().split("\\W+");

            TeachersReduceSideJoinWritable trsjw = new TeachersReduceSideJoinWritable();
            trsjw.setDatasetId(TeachersReduceSideJoinWritable.DPTOS_DATASET);
            trsjw.setDptoId(new LongWritable(Long.parseLong(splits[0])));

            context.write(trsjw, new Text(splits[1]));
        }
    }
}
