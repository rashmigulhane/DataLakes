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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExistenceoffileDriver extends Configured implements Tool{

	public void searchfilePath(String filename) throws Exception {
		// TODO Auto-generated method stub
		
		
		String[] args = {filename};
		
		
		 int res = ToolRunner.run(new Configuration(), new ExistenceoffileDriver(), args);
		 
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
		
		FileStatus[] status=	hdfs.listStatus(new Path("/metadata/"));
		
		ArrayList<String> filePathtobeSearched = new ArrayList<String>();
		for(FileStatus filenamee: status)
		{
			String filename = filenamee.getPath().toString();
			if(!filename.contains("category.txt"))
			{
			filePathtobeSearched.add(filename);
			}
			System.out.println(filename);
			
		}
		System.out.println("filetobesearched" + arg0[0]);
		
	JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName("WordCount");
        conf.set("filetobesearched", arg0[0].toLowerCase());
        
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(ExistenceoffileMapper.class);
        
        
        
        Path inp = new Path(filePathtobeSearched.get(0));
        
        String outputtopn = "/ExistenceoffilePath";
  	   if(hdfs.exists(new Path(outputtopn)))
  	    {
  	    	hdfs.delete(new Path(outputtopn));
  	    }
        
        Path out = new Path(outputtopn);
        //the hdfs input and output directory to be fetched from the command line
       
        
        
     //   FileInputFormat.addInputPath(conf, new Path(filePathtobeSearched.get(0)));
    //    FileInputFormat.addInputPath(conf, new Path(filePathtobeSearched.get(1)));
        
       for(String inputPathh: filePathtobeSearched)
        {
        FileInputFormat.addInputPath(conf, new Path(inputPathh));
        }
        
        FileOutputFormat.setOutputPath(conf, out);

        JobClient.runJob(conf);
        
        
        System.out.println("The files read are as foloow");
        
       String localOutputPath =  "/home/akanshahduser/Desktop/lakes/Preprocessing/ExistenceoffileDriver/a1.txt";
        hdfs.copyToLocalFile(false, new Path("/ExistenceoffilePath/part-00000"), new Path("/home/akanshahduser/Desktop/lakes/Preprocessing/ExistenceoffileDriver/a1.txt"),true);
    	BufferedReader bf =new BufferedReader(new FileReader(new File(localOutputPath)));
        String s=null;
		ArrayList<String> a=new ArrayList<String>(); 
		int i=0;
		while((s=bf.readLine())!=null){
		
			a.add(s);
			System.out.println(s+"\t"+i);
			i=i+1;
		}
		
		if(a.isEmpty())
		{
			System.out.println("No matching path found");
		}
		else
		{
		System.out.println("Do you want to Download files ..Y/N");
		Scanner sc=new Scanner(System.in);
		
		String choi = sc.next();
		if(choi.equals("Y"))
		{
			System.out.println("Enter the local file system path where you want to download files");
			String localpath = sc.next();
			System.out.println("type ALL for all files or SPI for specific file download");
			String choi1 = sc.next();
		if(choi1.equals("ALL"))
		{
			for(String val:a)
			{
				System.out.println(val);
				String localOutputdis = localpath+"/";
			
				Path p = new Path(val);
				
				
				localOutputdis = localOutputdis + p.getName();
				
			//	 hdfs.copyToLocalFile(false, new Path(val), new Path(localOutputdis),true);
				
				hdfs.copyToLocalFile(false, new Path(val.trim()), new Path(localOutputdis),true);
			}
			
			System.out.println("Files are successfully downloaded at location " + localpath);
		}
		if(choi1.equals("SPI"))
		{
			System.out.println("Enter the file number spearated by comma");
			String filenamee = sc.next();
			String[] allopt=filenamee.split(",");
			for(String ssqw:allopt)
			{
				String val =a.get(Integer.parseInt(ssqw));
				Path p = new Path(val);
				
				
				
				String localOutputdis = localpath+"/";
				localOutputdis = localOutputdis + p.getName();
				
				
				
			    hdfs.copyToLocalFile(false, new Path(a.get(Integer.parseInt(ssqw)).trim()), new Path(localOutputdis),true);
			    
			}
			System.out.println("Files are successfully downloaded at location " + localpath);
		}
		}
	
        //the following code will read the file and display to user
        
        /*
         * 
         * hdfs.copyToLocalFile(false, new Path(hdfsmeta), new Path(localOutputPath),true);
		BufferedReader bf =new BufferedReader(new FileReader(new File(localOutputPath)));
		String s=null;
		ArrayList<String> a=new ArrayList<String>(); 
		int i=0;
		System.out.println("Kindly enter the category number which you want to search");
		while((s=bf.readLine())!=null){
			a.add(s);
			System.out.println(s+"\t"+i);
			i=i+1;
		}
         * 
         * 
         * 
         * 
         */
        
		}
		return 0;
		
	}
		

}
