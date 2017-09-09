package mapdemo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
/**
 * @category 统计TF值 词语在对应文本中出现的频率
 * @author huangyueran
 *
 */
public class Reducer2 extends Reducer<Text, Text, Text, Text> {
 
    public Reducer2() {
    }
 
   /**
    * 输入格式:	doc2	:	can,1  得到的都是同一个key(也就是同一个文本)中的单词和单词次数
    */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumOfWordsInDocument = 0; // 文本中词的总个数
        Map<String, Integer> tempCounter = new HashMap<String, Integer>();
        for (Text val : values) {
            String[] wordCounter = val.toString().split(","); // [ ( can )  ( 1 )  ]
            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1])); // can  :  can出现的次数
            sumOfWordsInDocument += Integer.parseInt(val.toString().split(",")[1]); // 统计文本中词的总个数
        }
        for (String wordKey : tempCounter.keySet()) {
            context.write(new Text(wordKey + "," + key.toString()), new Text(tempCounter.get(wordKey) + "/"
                    + sumOfWordsInDocument)); 
			//            hive,doc1	5/25
			//            tom,doc1	3/25
			//            world,doc1	3/25
        }
    }
}
