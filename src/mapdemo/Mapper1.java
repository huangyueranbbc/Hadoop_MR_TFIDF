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
 * @category  以单词,文本名称为Key 输出map 
 * can,doc2	1
 * @author huangyueran
 *
 */
public class Mapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
 
    public Mapper1() {
    }
 
    private static Set<String> remove;
 
    /*
     * 取出没有意义的词
     */
    static {
        remove = new HashSet<String>();
        remove.add("I"); remove.add("a");
        remove.add("about"); remove.add("an");
        remove.add("are"); remove.add("as");
        remove.add("at"); remove.add("be");
        remove.add("by"); remove.add("com");
        remove.add("de"); remove.add("en");
        remove.add("for"); remove.add("from");
        remove.add("how"); remove.add("in");
        remove.add("is"); remove.add("it");
        remove.add("la"); remove.add("of");
        remove.add("on"); remove.add("or");
        remove.add("that"); remove.add("the");
        remove.add("this"); remove.add("to");
        remove.add("was"); remove.add("what");
        remove.add("when"); remove.add("where");
        remove.add("who"); remove.add("will");
        remove.add("with"); remove.add("and");
        remove.add("the"); remove.add("www");
    }
 
   
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Compile all the words using regex
        Pattern p = Pattern.compile("\\w+");
        Matcher m = p.matcher(value.toString()); //得到字符开头的词 如果不是字符开头则不是单词
 
        // Get the name of the file from the inputsplit in the context
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); // 获取文件的名称 doc1 doc2
 
        // build the values and write <k,v> pairs through the context
        // 构建输出内容
        StringBuilder valueBuilder = new StringBuilder();
        while (m.find()) { // 获取单词(字符开头)
            String matchedKey = m.group().toLowerCase(); // 获取第一组正则表达式的匹配值 并转小写
            // remove names starting with non letters, digits, considered stopwords or containing other chars
            // 如果是单个字母isDigit 数字开头isDigit 是无意义的词 是下划线 则不进行统计,跳过该单词
            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
                    || remove.contains(matchedKey) || matchedKey.contains("_")) {
                continue;
            }
            // 拼接Map输出的内容 can,doc2
            valueBuilder.append(matchedKey);
            valueBuilder.append(",");
            valueBuilder.append(fileName);
            // emit the partial <k,v>
            context.write(new Text(valueBuilder.toString()), new IntWritable(1)); // map输出格式:	can,doc2	1
            valueBuilder.setLength(0);
        }
    }
}

