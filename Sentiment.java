package cisc.mapreduce;



// written by Hannah Wilkinson, 10026110

///resources include: https://www.cloudera.com/documentation/other/tutorial/CDH5/topics/ht_example_4_sentiment_analysis.html
///https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html
// Input format: exactly as specified by Professor Martin. 
//cd /caslab_linux/caslab/hadoop-2.7.3/bin

//Input: ./hadoop jar your_jar_file_path /cisc432/negative.txt  /cisc432/positive.txt /cisc432/data.txt output_path

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sentiment {
	enum Gauge {
		POSITIVE, NEGATIVE
	}

	public static Set<String> goodWords = new HashSet<String>();
	public static Set<String> badWords = new HashSet<String>();

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text productID = new Text();
		private Text body = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			String[] strings = line.split("\t", -1);
			if (strings.length == 8) {
				try {

					productID.set(strings[1]);
					body.set(strings[7]);
					context.write(productID, body);

				} catch (Exception e) {

				}
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			int counter = 0;
			for (Text val : value) {
				String review = val.toString();
				String review2 = review.replaceAll("[-+.^:,]", "");
				String[] arr = review2.split(" ");
				for (String x : arr) {
					if (goodWords.contains(x)) {
						counter = counter + 1;
					}
					// Filter and count "bad" words.
					if (badWords.contains(x)) {
						counter = counter - 1;
					}
				}

			}

			if (counter > 0) {
				result.set("positive");
			} else if (counter < 0) {
				result.set("negative");
			} else
				result.set("neutral");

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Sentiment");
		job.setJarByClass(Sentiment.class);

		job.setMapperClass(TokenizerMapper.class);

		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		File file = new File(args[0]);
		File file2 = new File(args[1]);

		try {
			BufferedReader fis = new BufferedReader(new FileReader(file2));
			String goodWord;
			while ((goodWord = fis.readLine()) != null) {
				goodWords.add(goodWord);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing cached file '");
		}

		try {
			BufferedReader fis = new BufferedReader(new FileReader(file));
			String badWord;
			while ((badWord = fis.readLine()) != null) {
				badWords.add(badWord);
			}
		} catch (IOException ioe) {
			System.err.println("Caught exception parsing cached file '");
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
