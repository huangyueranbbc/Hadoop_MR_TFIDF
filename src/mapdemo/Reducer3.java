package mapdemo;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @category 计算TF-IDF值
 * @author huangyueran
 *
 */
public class Reducer3 extends Reducer<Text, Text, Text, Text> {

	private static final DecimalFormat DF = new DecimalFormat("###.########");

	private Text wordAtDocument = new Text();

	private Text tfidfCounts = new Text();

	public Reducer3() {
	}

	/**
	 * key: hive value: doc1=5/25
	 */
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int numDocs = context.getConfiguration().getInt("numberOfDocsInCorpus", 0); // 统计文档的个数

		int keyAppears = 0; // 文档中出现词的文档个数
		
		Map<String, String> tempFrequencies = new HashMap<String, String>(); // 词频 TF值
		
		for (Text val : values) { // doc1=5/25
			String[] temp = val.toString().split("="); // doc1 5/25

			if (Integer.parseInt(temp[1].split("/")[0]) > 0) { // 如果单词出现次数大于0
				keyAppears++;
			}
			tempFrequencies.put(temp[0], temp[1]); // doc1 : 5/25	该单词在doc1文档中的词频TF
		}
		for (String document : tempFrequencies.keySet()) {
			String[] temp1 = tempFrequencies.get(document).split("/"); // 5 25

			// 计算词频TF值 5/25
			double tf = Double.valueOf(Double.valueOf(temp1[0]) / Double.valueOf(temp1[1]));

			// 计算逆向文件频率 IDF值 
			// 拉普拉斯平滑	(keyAppears == 0 ? 1 : 0) + keyAppears 如果词一次都没有出现 则进行+1操作
			double idf = Math.log10((double) numDocs / (double) ((keyAppears == 0 ? 1 : 0) + keyAppears));

			// 计算TF-IDF值
			double tfIdf = tf * idf;

			this.wordAtDocument.set(key + "@" + document);
			this.tfidfCounts.set("[" + keyAppears + "/" + numDocs + " , " + temp1[0] + "/" + temp1[1] + " , "
					+ DF.format(tfIdf) + "]");

			context.write(this.wordAtDocument, this.tfidfCounts);
			//					can@doc2	[1/2 , 1/29 , 0.01038034]
			//					cat@doc2	[2/2 , 2/29 , 0]
			//					cat@doc1	[2/2 , 3/25 , 0]
			//					git@doc2	[1/2 , 1/29 , 0.01038034]
			//					hadoop@doc2	[2/2 , 1/29 , 0]
			//					hadoop@doc1	[2/2 , 3/25 , 0]
			//					hello@doc2	[2/2 , 2/29 , 0]
			//					hello@doc1	[2/2 , 5/25 , 0]
			//					hibernate@doc2	[1/2 , 5/29 , 0.05190172]
			//					hive@doc2	[2/2 , 3/29 , 0]
			//					hive@doc1	[2/2 , 5/25 , 0]
			//					hyr@doc2	[1/2 , 1/29 , 0.01038034]
			
		}
	}
}
