
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class appendd {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String fileuri = "/op/cricket/a1.txt";
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:54310"),conf);
				FSDataOutputStream out = fs.append(new Path(fileuri));
				PrintWriter writer = new PrintWriter(out);
				writer.append("I am appending this to my file");
				writer.close();
				fs.close();

	}

}
