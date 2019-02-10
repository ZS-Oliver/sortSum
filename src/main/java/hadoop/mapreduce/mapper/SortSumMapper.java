package hadoop.mapreduce.mapper;

import hadoop.mapreduce.bean.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {


    // 读取日志中的一行数据，切分各个字段，抽取需要的字段封装成kv
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields = StringUtils.split(line, " ");

        String phone = fields[1];
        long upFlow = Long.parseLong(fields[2]);
        long downFlow = Long.parseLong(fields[3]);

        context.write(new Text(phone), new FlowBean(phone, upFlow, downFlow));

    }
}
