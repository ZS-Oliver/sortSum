package hadoop.mapreduce.reducer;

import hadoop.mapreduce.bean.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    // 数据<phone,Flowbean,Flowbean...>
    // reduce 遍历values,累加求和
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long upFlowCounter = 0;
        long downFlowCounter = 0;

        for (FlowBean bean : values) {

            upFlowCounter += bean.getUpFlow();
            downFlowCounter += bean.getDownFlow();
        }

        context.write(key, new FlowBean(key.toString(), upFlowCounter, downFlowCounter));

    }
}
