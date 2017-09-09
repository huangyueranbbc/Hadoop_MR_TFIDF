package mapdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
 * @category 统计单词在对应文档中出现的次数
 * @author huangyueran
 *
 */
public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    public Reducer1() {
    }
 
    /**
     * wordcount 统计单词次数
     */
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
 
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        //write the key and the adjusted value (removing the last comma)
        context.write(key, new IntWritable(sum));
    }
}