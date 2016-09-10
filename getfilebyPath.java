import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class getfilebyPath extends Configured implements Tool{
	public void ViewAllfromdirectory(String directoryPath) throws Exception  {
		// TODO Auto-generated method stub
		String[] args = {directoryPath};
		
	
		 int res = ToolRunner.run(new Configuration(), new getfilebyPath(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		  ArrayList<String> allfiles = null;
		System.out.println(arg0[0]);
		String[]  parts = arg0[0].split("/");
		int i=1;
		String filenamepath ="";
		while(i<parts.length && i<3)
		{
			if(i==3)
			{
				 filenamepath = filenamepath + parts[i];
			}
			if(i==(parts.length-1))
			{
				 filenamepath = filenamepath + parts[i];
			}
			else
			{
				filenamepath = filenamepath + parts[i]+"_";
			}
		 i=i+1;
		}
		
		
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
		
		FileStatus[] status=	hdfs.listStatus(new Path("/metadata/"));
		System.out.println(status[0]);
		ArrayList<String> filePathtobeSearched = new ArrayList<String>();
		for(FileStatus filenamee: status)
		{
			String filename = filenamee.getPath().getName();
			if(filename.startsWith(filenamepath))
			{
				filePathtobeSearched.add(filename);
			}
			
		}
		
		if(parts.length<4)
		{
			//print all the files from specific folder
			
			for(String filepath : filePathtobeSearched)
			{
			String localOutputPath = "/home/akanshahduser/Desktop/lakes/Preprocessing/a1.txt";
			File file = new File(localOutputPath);
	 	    if(file.exists())
	 	    {
	 	    	file.delete();
	 	    	
	 	    	
	 	    }
	 	    
	 	   hdfs.copyToLocalFile(false, new Path("/metadata/"+filepath), new Path(localOutputPath),true);
	 	   
	 	   
	 	   
	 	  BufferedReader bf =new BufferedReader(new FileReader(new File(localOutputPath)));
			String s=null;
			allfiles=new ArrayList<String>(); 
			int ii=0;
			System.out.println("Kindly enter the category number which you want to search");
			while((s=bf.readLine())!=null){
				allfiles.add(s);
				System.out.println(ii+"\t"+s);
				ii=ii+1;
			}
	 	    
			}
	 	    
			
			
		}
		else
		{
		System.out.println(filePathtobeSearched);
		
		JobConf conf = new JobConf(getConf(), getfilebyPath.class);
		conf.setJobName("Pathtosearch");
        conf.set("Path", arg0[0]);
       
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(ALLfilesPathfinder.class);
       
        Path inp = new Path("/metadata/"+filePathtobeSearched.get(0));
        
        String outputtopn = "/getALLfilefromDirectory";
 	   if(hdfs.exists(new Path(outputtopn)))
 	    {
 	    	hdfs.delete(new Path(outputtopn));
 	    }
        
        
        Path out = new Path(outputtopn);
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(conf, inp);
        FileOutputFormat.setOutputPath(conf, out);

        JobClient.runJob(conf);
        
		//use map reduce
		}
		
		System.out.println("Do you want to download All the files type Y else Type N");
		Scanner sc=new Scanner(System.in);
		
	String yourchoice = sc.next();
	if(yourchoice.equals("Y"))
	{
		
	}
	else if(yourchoice.equals("N"))
	{
		
	}
		
		return 0;
	}

}
