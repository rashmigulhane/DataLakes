

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class filedownload {
	
	public static void main(String args[]) throws IOException, URISyntaxException
	{
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:54310"), configuration);
		System.out.println("Specify the path you want to download files from hdfs");
		Scanner sc=new Scanner(System.in);
		
		String hdfs_source = sc.next();
		System.out.println("Specify the path you want to download files from hdfs");
		String local_source = sc.next();
		
		//finding hdsf path is a directory
		
	boolean isdir =	hdfs.isDirectory(new Path(hdfs_source));
	if (!isdir)
	{
		
	}
	else
	{
		
		hdfs.copyToLocalFile(false, new Path("/op/cricket/a1.txt"), new Path("/home/abc/a1.txt"),true);
	}
	}
}
