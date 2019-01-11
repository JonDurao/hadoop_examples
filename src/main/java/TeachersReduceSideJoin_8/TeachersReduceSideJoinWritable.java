package TeachersReduceSideJoin_8;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TeachersReduceSideJoinWritable implements WritableComparable<TeachersReduceSideJoinWritable> {
    public final static LongWritable DPTOS_DATASET = new LongWritable(0);
    public final static LongWritable TCHRS_DATASET = new LongWritable(1);
    private LongWritable dptoId, datasetId;

    public TeachersReduceSideJoinWritable() {
        dptoId = new LongWritable();
        datasetId = new LongWritable();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dptoId.write(dataOutput);
        datasetId.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dptoId.readFields(dataInput);
        datasetId.readFields(dataInput);
    }

    public TeachersReduceSideJoinWritable(LongWritable dptoId, LongWritable datasetId) {
        this.dptoId = dptoId;
        this.datasetId = datasetId;
    }

    public LongWritable getDptoId() {
        return dptoId;
    }

    public void setDptoId(LongWritable dptoId) {
        this.dptoId = dptoId;
    }

    public LongWritable getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(LongWritable datasetId) {
        this.datasetId = datasetId;
    }

    @Override
    public int compareTo(TeachersReduceSideJoinWritable o) {
        if (dptoId.compareTo(o.getDptoId()) == 0)
            return datasetId.compareTo(o.getDatasetId());
        else
            return dptoId.compareTo(o.getDptoId());
    }
}
