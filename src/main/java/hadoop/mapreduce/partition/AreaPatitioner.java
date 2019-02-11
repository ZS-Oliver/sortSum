package hadoop.mapreduce.partition;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class AreaPatitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

    private static HashMap<String, Integer> areaMap = new HashMap<>();

    // 静态代码块提前加载对应信息
    static {
        getAreaByCode(areaMap);
    }

    private static void getAreaByCode(HashMap<String, Integer> areaMap) {
        areaMap.put("12", 0);
        areaMap.put("13", 1);
        areaMap.put("14", 2);
    }


    @Override
    public int getPartition(KEY key, VALUE value, int i) {

        // 按照key分类，不同的类别返回不同的组号
        int areaCoder = areaMap.get(key.toString().substring(0, 2)) == null ? 3 : areaMap.get(key.toString().substring(0, 2));
        return areaCoder;
    }

}
