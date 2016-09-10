import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExistenceoffileMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	
	 private static String  N;
     public void configure(JobConf job) {
         N = (job.get("filetobesearched"));
     }
	
	
	
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		
		
		 String line = value.toString();
		
		 System.out.println(N);
		
		 //line =line.toLowerCase();
		
		 String[] line1 = line.split("\t");
		 
		 String filename = line1[1];
		
		 String[] actualname = filename.split("###");
		 String lenght = actualname[0];
		
		 String[] finalll = lenght.split("/");
		 String filesr = finalll[finalll.length-1];
		 filesr = filesr.toLowerCase();
		 if(filesr.startsWith(N))
		 {
			 output.collect(new Text(lenght), new Text(""));
			 
			 
		//  output.collect(new Text("FOUND"), new Text(line));
		 }
		
	}
	

}
