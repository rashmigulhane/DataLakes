import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class searchwithspecificReqDriver extends Configured implements Tool{

	public void getfilewiththisconstraints(String category, String year, String month, String date, String fileformat,String username, String size, String timestmp) throws Exception {
		// TODO Auto-generated method stub
		
		
		String[] args = {category,year,month,date,fileformat,username,size,timestmp};
		
		
		 int res = ToolRunner.run(new Configuration(), new searchwithspecificReqDriver(), args);
		
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		String pathtoSearch = "";
		
		//get all files of metadaa first
		
		
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
		
		FileStatus[] status=	hdfs.listStatus(new Path("/metadata/"));
		
		ArrayList<String> filePathtobeSearched = new ArrayList<String>();
		for(FileStatus filenamee: status)
		{
			String filename = filenamee.getPath().getName().toString();
			if(!filename.contains("category.txt"))
			{
			filePathtobeSearched.add(filename);
			}
			System.out.println(filename);
			
		}
		
		ArrayList<String> filewhichContainsconstraint = new ArrayList<String>();
		String pathtofind;
		
		if(((arg0[0].equals("NA"))) && ((arg0[1].equals("NA"))) && ((arg0[2].equals("NA"))) )
		{
			
			for(String fn:filePathtobeSearched)
			{
				
				filewhichContainsconstraint.add(fn);
				
			}
		}
		
		else if(((arg0[0].equals("NA"))) && ((arg0[1].equals("NA"))) && (!(arg0[2].equals("NA"))))
		{
			String psd =arg0[2];
			for(String fn:filePathtobeSearched)
			{
				if(fn.contains(psd))
				{
					filewhichContainsconstraint.add(fn);
				}
			}
		}
		
		else if(((arg0[0].equals("NA"))) && (!(arg0[1].equals("NA"))) && ((arg0[2].equals("NA"))))
		{
			String psd =arg0[1];
			for(String fn:filePathtobeSearched)
			{
				if(fn.contains(psd))
				{
					filewhichContainsconstraint.add(fn);
				}
			}
		}
		
		else if((!(arg0[0].equals("NA"))) && ((arg0[1].equals("NA"))) && ((arg0[2].equals("NA"))))
		{
			String psd =arg0[0];
			for(String fn:filePathtobeSearched)
			{
				if(fn.startsWith(psd))
				{
					filewhichContainsconstraint.add(fn);
				}
			}
		}
		
		else if(((arg0[0].equals("NA"))) && (!(arg0[1].equals("NA"))) && (!(arg0[2].equals("NA"))))
		{
			String psd =arg0[1];
			String psd1 =arg0[2];
			for(String fn:filePathtobeSearched)
			{
				if(fn.startsWith(psd) && fn.contains(psd1))
				{
					filewhichContainsconstraint.add(fn);
				}
			}
		}
		
		else if((!(arg0[0].equals("NA"))) && ((arg0[1].equals("NA"))) && (!(arg0[2].equals("NA"))))
		{
			String psd =arg0[0];
			String psd1 =arg0[2];
			for(String fn:filePathtobeSearched)
			{
				if(fn.startsWith(psd) && fn.contains(psd1))
				{
					filewhichContainsconstraint.add(fn);
				}
			}
		}
		
		else if((!(arg0[0].equals("NA"))) && (!(arg0[1].equals("NA"))) && ((arg0[2].equals("NA"))))
		{
			String psd =arg0[0];
			String psd1 =arg0[1];
			for(String fn:filePathtobeSearched)
			{
				if(fn.startsWith(psd) && fn.contains(psd1))
				{
					filewhichContainsconstraint.add(fn);
				}
			}
		}
		else if((!(arg0[0].equals("NA"))) && (!(arg0[1].equals("NA"))) && (!(arg0[2].equals("NA"))))
		{
			String psd =arg0[0];
			String psd1 =arg0[1];
			String psd2 =arg0[2];
			for(String fn:filePathtobeSearched)
			{
				if(fn.startsWith(psd) && fn.contains(psd1)  && fn.contains(psd2))
				{
					filewhichContainsconstraint.add(fn);
				}
			}
		}
		if(filewhichContainsconstraint.size()>0)
		{
			
			for(String fds:filewhichContainsconstraint)
			{
			System.out.println(fds);
			}
			
			
			JobConf conf = new JobConf(getConf(), WordCount.class);
	        conf.setJobName("WordCount");
	        conf.set("date", arg0[3]);
	    
	        conf.set("fileformat", arg0[4]);
	        conf.set("username", arg0[5]);
	        conf.set("size", arg0[6]);
	        conf.set("timestmp", arg0[7]);
	       
	        conf.setOutputKeyClass(Text.class);
	        conf.setOutputValueClass(Text.class);
	        conf.setMapperClass(searchwithspecificReqMapper.class);
	        
	        String outputtopn = "/searchwithspecificReq";
	   	   if(hdfs.exists(new Path(outputtopn)))
	   	    {
	   	    	hdfs.delete(new Path(outputtopn));
	   	    }
	   	  Path out = new Path(outputtopn);
	   	 for(String inputPathh: filewhichContainsconstraint)
	        {
	        FileInputFormat.addInputPath(conf, new Path("/metadata/"+inputPathh));
	        }
	   	 
	   	 
	     FileOutputFormat.setOutputPath(conf, out);

	      JobClient.runJob(conf);
	        
	        
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
		}
		else
		{
		System.out.println("Sorry No files matching to your Query");
		
		}
		
		
		
		//creating path for metastore selection file
		return 0;
	}

}
