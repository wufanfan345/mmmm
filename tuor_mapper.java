package iflytek.tuorism;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.StringTokenizer;

public class tuor_mapper extends Mapper<Object, Text, Text, NullWritable> {

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // 跳过
        if (line.startsWith("UserID")) {
            return;
        }

        // 使用逗号分割每一行数据
        StringTokenizer tokenizer = new StringTokenizer(line, ",");

        // 提取各个字段并处理空值
        String userID = getNextTokenOrDefault(tokenizer, "0");
        String takenProduct = getNextTokenOrDefault(tokenizer, "No");
        String yearlyAvgView = getNextTokenOrDefault(tokenizer, "0");
        String preferredDevice = getNextTokenOrDefault(tokenizer, "Unknown");
        String totalLikes = getNextTokenOrDefault(tokenizer, "0");
        String yearlyAvgCheckins = getNextTokenOrDefault(tokenizer, "0");
        String memberInFamily = getNextTokenOrDefault(tokenizer, "0");
        String preferredLocationType = getNextTokenOrDefault(tokenizer, "Unknown");
        String yearlyAvgComment = getNextTokenOrDefault(tokenizer, "0");
        // 添加更多字段的处理

        // 生成唯一的行键
        String rowKey = userID;

        // 拼接成适合写入HBase的格式
        String result = String.join(",",
                rowKey, takenProduct, yearlyAvgView, preferredDevice, totalLikes,
                yearlyAvgCheckins, memberInFamily, preferredLocationType, yearlyAvgComment);

        context.write(new Text(result), NullWritable.get());
    }

    private String getNextTokenOrDefault(StringTokenizer tokenizer, String defaultValue) {
        if (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            return token.isEmpty() ? defaultValue : token;
        } else {
            return defaultValue;
        }
    }
}