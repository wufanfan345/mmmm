package iflytek.tuorism;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import java.io.IOException;

public class tuor_Job {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Customer behaviour Tourism");
        job.setJarByClass(tuor_Job.class);

        job.setMapperClass(tuor_mapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableReducerJob("customer_behaviour_tourism", tuor_reduce.class, job);

        // 设置文件输入路径
        FileInputFormat.addInputPath(job, new Path("/user/hadoop/mapreduce/ds/Customer_behaviour_Tourism.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}