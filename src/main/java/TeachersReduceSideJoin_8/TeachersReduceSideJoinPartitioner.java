package TeachersReduceSideJoin_8;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TeachersReduceSideJoinPartitioner extends Partitioner<TeachersReduceSideJoinWritable, Text> {

    @Override
    public int getPartition(TeachersReduceSideJoinWritable teachersReduceSideJoinWritable, Text text, int i) {
        return Math.abs(teachersReduceSideJoinWritable.getDptoId().hashCode()) % i;
    }
}
