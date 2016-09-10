import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class searchwithspecificReqMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	
	
	
     
     
     
     private static String  date;
     private static String  fileformat;
     private static String  username;
     private static String  size;
     private static String  timestmp;
     public void configure(JobConf job) {
    	 date = (job.get("date"));
    	 fileformat = (job.get("fileformat"));
    	 username = (job.get("username"));
    	 size = (job.get("size"));
    	 timestmp = (job.get("timestmp"));
     }
	
	
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		 String line = value.toString();
		 String[] linee = line.split("\t");
		 String[]  records =linee[1].split("###");
		 String path =records[0];
		 String format =linee[0];
		 String datee_timestamp =records[1];
		 String size1 =records[2];
		 String user =records[3];
		 String[] details_path =path.split("/");
		 String todatday=details_path[3];
		 boolean pathf =false;
		 boolean formatf =false;
		 boolean datee_timestampf =false;
		 boolean sizef =false;
		 boolean userf=false;
		 boolean todatdayf=false;
		 if(timestmp.equals("NA"))
		 {
			 datee_timestampf =false;
		 }
		 else
		 {
			 if(timestmp.startsWith(datee_timestamp))
			 {
				 datee_timestampf =false;
			 }
			 else
			 {
				 datee_timestampf =true; 
			 }
		 }
		 
		 
		 
		 if(date.equals("NA"))
		 {
			 todatdayf =false;
		 }
		 else
		 {
			 if(date.equals(todatday))
			 {
				 todatdayf =false;
			 }
			 else
			 {
				 todatdayf =true; 
			 }
		 }
		 
		 
		 
		 
		 
		 if(fileformat.equals("NA"))
		 {
			 formatf =false;
		 }
		 else
		 {
			 if(fileformat.equalsIgnoreCase(format.trim()))
			 {
				 formatf =false;
			 }
			 else
			 {
				 formatf =true; 
			 }
		 }
		 
		 
		 
		 if(username.equals("NA"))
		 {
			 userf =false;
		 }
		 else
		 {
			 if(username.equalsIgnoreCase(user))
			 {
				 userf =false;
			 }
			 else
			 {
				 userf =true; 
			 }
		 }
		 
		 
		 
		 
		 if(size.equals("NA"))
		 {
			 sizef =false;
		 }
		 else
		 {
			 if(size.equals(size1))
			 {
				 sizef =false;
			 }
			 else
			 {
				 sizef =true; 
			 }
		 }
		 
		 
		 if((sizef==false) && (userf==false) && (formatf==false) && (datee_timestampf==false))
		 {
			 output.collect(new Text(path), new Text(""));
		 }
		
		 System.out.println("************************************************************");
		 
		 
		 
		
	}

}
