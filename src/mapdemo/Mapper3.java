package mapdemo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
/**
 * @category 以(单词)为key 	(文本名称=单词在该文本出现的TF值)为value 输出map
 * @author huangyueran
 *
 */
public class Mapper3 extends Mapper<LongWritable, Text, Text, Text> {
 
    public Mapper3() {
    }
 
    /**
     * hive,doc1		5/25
	 *	tom,doc1		3/25
	 *	world,doc1	3/25
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndCounters = value.toString().split("\t");  // ( hive,doc1 ) ( 5/25 )
        String[] wordAndDoc = wordAndCounters[0].split(",");   // ( hive ) ( doc1 )              
        context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1])); // 以(单词)为key 	(文本名称=单词在该文本出现的TF值)为value
        // hive		:		doc1=5/25
    }
}
