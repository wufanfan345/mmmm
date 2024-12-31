package iflytek.tuorism;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class tuor_reduce extends TableReducer<Text, NullWritable, NullWritable> {

    public String columnFamily = "f";

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        List<String> list = Arrays.asList(key.toString().split(","));

        String rowKey = list.get(0); // 第一列是主键

        String[] columns = {"taken_product", "yearly_avg_view", "preferred_device", "total_likes",
                "yearly_avg_checkins", "member_in_family", "preferred_location_type", "yearly_avg_comment"};
        // 添加更多字段的列名

        Put outPutValue = new Put(Bytes.toBytes(rowKey));

        for (int i = 1; i < list.size(); i++) {
            outPutValue.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns[i - 1]), Bytes.toBytes(list.get(i)));
        }

        context.write(NullWritable.get(), outPutValue);
    }
}