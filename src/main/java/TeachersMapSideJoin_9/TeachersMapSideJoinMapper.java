package TeachersMapSideJoin_9;

import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

// public class NgramsMapper extends Mapper {
public class TeachersMapSideJoinMapper extends Mapper <LongWritable, Text, NullWritable, Text> {
    private Map<Integer, String> dptosMap = new HashMap<Integer, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI dptosFile = context.getCacheFiles()[0];
        fillDptosMap(dptosFile, context);
    }

    private void fillDptosMap(URI dptoFile, Mapper<LongWritable, Text, NullWritable, Text>.Context context) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(new Path(dptoFile))));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] splits = line.split("\\W+");
                dptosMap.put(Integer.parseInt(splits[0]), splits[1]);

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        String[] splitsTeacher = value.toString().split("\\W+");

        sb.append(splitsTeacher[0]).append("-").append(splitsTeacher[2]).append("-").append(splitsTeacher[3]).append("-").append(dptosMap.get(Integer.parseInt(splitsTeacher[1])));
        context.write(NullWritable.get(), new Text(sb.toString()));
    }
}
