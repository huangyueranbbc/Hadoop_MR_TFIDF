package mapdemo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
/**
 * @category 以文本为key 输出map
 * @author huangyueran
 *
 */
public class Mapper2 extends Mapper<LongWritable, Text, Text, Text> {
 
    public Mapper2() {
    }
 
    // 输入内容格式
	//    can,doc2	1
	//    cat,doc1	3
	//    cat,doc2	2
	//    git,doc2	1
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndDocCounter = value.toString().split("\t"); // 得到	( can,doc2 )	( 1 )
        String[] wordAndDoc = wordAndDocCounter[0].split(","); // 得到 ( can ) ( doc2 )
        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "," + wordAndDocCounter[1])); // 以文本名称问key  单词,词语次数为value 方便统计文本中词的总个数
        // doc2	:	can,1
    }
}
