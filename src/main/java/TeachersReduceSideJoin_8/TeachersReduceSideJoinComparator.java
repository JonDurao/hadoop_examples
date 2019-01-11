package TeachersReduceSideJoin_8;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TeachersReduceSideJoinComparator extends WritableComparator {
    public TeachersReduceSideJoinComparator() {
        super(TeachersReduceSideJoinWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TeachersReduceSideJoinWritable trsjwa = (TeachersReduceSideJoinWritable) a;
        TeachersReduceSideJoinWritable trsjwb = (TeachersReduceSideJoinWritable) b;
        return trsjwa.getDptoId().compareTo(trsjwb.getDptoId());
    }
}
