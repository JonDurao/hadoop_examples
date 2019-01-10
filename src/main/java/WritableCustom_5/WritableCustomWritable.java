package WritableCustom_5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableCustomWritable implements WritableComparable<WritableCustomWritable> {
    private DoubleWritable first, second;

    public WritableCustomWritable() {
        first = new DoubleWritable();
        second = new DoubleWritable();
    }

    public WritableCustomWritable(Double first, Double second) {
        this.first = new DoubleWritable(first);
        this.second = new DoubleWritable(second);
    }

    @Override
    public int compareTo(WritableCustomWritable writableCustomWritable) {
        if (first.compareTo(writableCustomWritable.getFirst()) == 0)
            return second.compareTo(writableCustomWritable.getSecond());

        return first.compareTo(writableCustomWritable.getFirst());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    public DoubleWritable getFirst() {
        return first;
    }

    public void setFirst(DoubleWritable first) {
        this.first = first;
    }

    public DoubleWritable getSecond() {
        return second;
    }

    public void setSecond(DoubleWritable second) {
        this.second = second;
    }
}
