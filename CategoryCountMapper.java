import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CategoryCountMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>{

	
	 public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
     {
           //taking one line at a time from input file and tokenizing the same
           String line = value.toString();
          
        
         //iterating through all the words available in that line and forming the key value pair
           while (tokenizer.hasMoreTokens())
           {
              word.set(tokenizer.nextToken());
              //sending to output collector which inturn passes the same to reducer
                output.collect(word, one);
                //output.collect(word, N);
                
           }
      }
	
	
	
}
