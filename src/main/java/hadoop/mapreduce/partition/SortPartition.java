package hadoop.mapreduce.partition;

import hadoop.mapreduce.bean.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * 将原始数据划分到不同文件
 * 需要自定义两个机制
 * 1、改造分区的逻辑，自定义一个partition
 * 2、自定义reducer task 的并发任务数
 */
public class SortPartition {

    public static class AreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

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

    public static class AreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

            long upFlowCount = 0;
            long downFlowCount = 0;

            for (FlowBean bean : values) {
                upFlowCount += bean.getUpFlow();
                downFlowCount += bean.getDownFlow();
            }

            context.write(key, new FlowBean(key.toString(), upFlowCount, downFlowCount));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SortPartition.class);

        job.setMapperClass(AreaMapper.class);
        job.setReducerClass(AreaReducer.class);

        // 设置自定义的分组逻辑定义
        job.setPartitionerClass(AreaPatitioner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 设置reduce的任务并发数，和分组的数量保持一致
        job.setNumReduceTasks(4);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
