

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

public class getfilebycategory_driver extends Configured implements Tool {
	public String choicecat;
	public void findfiles(String choice) throws Exception  {
		// TODO Auto-generated method stub
		String[] args = {choice};
		
		choicecat = choice;
		 int res = ToolRunner.run(new Configuration(), new getfilebycategory_driver(), args);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		//first get all the files from filesystem with this match category
		
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
		
		FileStatus[] status=	hdfs.listStatus(new Path("/metadata/"));
		ArrayList<String> inputtosearched = new ArrayList<String>();
		choicecat = arg0[0];
		for(FileStatus filenamee: status)
		{
			String filename = filenamee.getPath().getName();
			if(filename.startsWith(choicecat))
			{
				inputtosearched.add(filenamee.getPath().toString());
			}
			
		}
		
		
		
		for(String filenamee: inputtosearched)
		{
			System.out.println(filenamee);
		}
		
		
		
		String localOutputPath = "/home/akanshahduser/Desktop/lakes/Preprocessing/a1.txt";
		File file = new File(localOutputPath);
 	    if(file.exists())
 	    {
 	    	file.delete();
 	    	
 	    	
 	    }
 	    
 	   ArrayList<String> allfiles = null;
 	   for(String filenamee: inputtosearched)
 	   {
		
		hdfs.copyToLocalFile(false, new Path(filenamee), new Path(localOutputPath),true);
		
		BufferedReader bf =new BufferedReader(new FileReader(new File(localOutputPath)));
		String s=null;
		allfiles=new ArrayList<String>(); 
		int i=0;
		System.out.println("Kindly enter the category number which you want to search");
		while((s=bf.readLine())!=null){
			allfiles.add(s);
			System.out.println(i+"\t"+s);
			i=i+1;
		}
		
 	   }
 	   
 	   System.out.println("Do you want to download all this files to your local system type ALL or SPI for specific files");
 	   {
 		   Scanner sc=new Scanner(System.in);
 		   String choice = sc.next();
 		   if (choice.equals("ALL"))
 		   {
 			   
 			   System.out.println("\nWhat is the lcoation of input path");
 			  String inputLocation = sc.next();
 			   for(String path:allfiles)
 			   {
 				String[]  parts = path.split("\t");
 				String[] alldetails = parts[1].split("###");
 				System.out.println(alldetails[0]);
 				
 				
 				
 				hdfs.copyToLocalFile(false, new Path(alldetails[0]), new Path(inputLocation),true);
 			   }
 			  
 			  
 		   }
 		  if (choice.equals("SPI"))
 		  {
 			 System.out.println("Kindly enter the file Sequence separated by comma");
			 String inputLocation = sc.next();
			  System.out.println("\nWhat is the lcoation of input path");
 			  String filespath = sc.next();
			 String[]  fileID = inputLocation.split(",");
			 int i=0;
			 while(i<fileID.length)
			 {
				 System.out.println(fileID[i]);
				
				 String path = allfiles.get(i).toString();
					String[]  parts = path.split("\t");
	 				String[] alldetails = parts[1].split("###");
	 				System.out.println(alldetails[0]);
					
			//	 String path = fileID[i];
				 
			
	 				
	 				
	 				
	 			hdfs.copyToLocalFile(false, new Path(alldetails[0]), new Path(filespath),true);
				 i=i+1;
			 }
			
 		  }
 	   }
		
		
		
	/*	
		JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName("WordCount");
        conf.set("test", "123");
        //Setting configuration object with the Data Type of output Key and Value
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        //Providing the mapper and reducer class names
        conf.setMapperClass(CategoryCountMapper.class);
       conf.setReducerClass(CategoryCountReducer.class);
        //We wil give 2 arguments at the run time, one in input path and other is output path
        Path inp = new Path(args[0]);
        Path out = new Path(args[1]);
        //the hdfs input and output directory to be fetched from the command line
        FileInputFormat.addInputPath(conf, inp);
        FileOutputFormat.setOutputPath(conf, out);

        JobClient.runJob(conf);
        */
        return 0;
	}

}
