import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ALLfilesPathfinder extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	
	 private static String  N;
     public void configure(JobConf job) {
         N = (job.get("Path"));
     }
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		// TODO Auto-generated method stub
		
		 String line = value.toString();
		 System.out.println(N);
		 if(line.contains(N))
		 {
		  output.collect(new Text(line), new Text("##"));
		 }
		
	}

}
