

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class filewritetohdfs {
private ArrayList<String> bestwords;



public static void main(String args[]) throws Exception
{
    
	   
	   //for unlabeeled files:
	filewritetohdfs obj1 = new filewritetohdfs(); 
	   
	   //we will 
	
	String global_choice;
	  String cont_choice;
	  do
	   {
		  Scanner sc=new Scanner(System.in); 
		  System.out.println("Please select any of the following option");
		   System.out.println("1 : Loading of files with user defined category ");
		   System.out.println("2 :  Loading of files with Automated category.Only Text files are given automatic category else would be assigned to default");
		   System.out.println("3 : Enter month to constraint on search Results ");
		   System.out.println("4 :Enter day to constraint on search Results ");
		   System.out.println("5  :Enter fileformat to constraint on search Results ");
		   System.out.println("6  :Enter username to constraint on search Results ");
		   System.out.println("7  :Enter size of file to constraint on search Results ");
		   System.out.println("8  :Enter timestamp when file is created in the format yyyyy-mm-dd hh:mm:ss ");
		   System.out.println("Enter your choice");
		   global_choice = sc.next();
		   if(global_choice.equals("1"))
		   {
			   obj1.UpoadFiletoHDFS();
		   }
		   if(global_choice.equals("2"))
		   {
			   obj1.Automatictagging();
		   }
		   if(global_choice.equals("3"))
		   {
			   obj1.searchwithspecificReq();
		   }
		   
		   System.out.println("Do you want to continue,Enter N to exit Y to continue");
			 
		  cont_choice = sc.next();
	   }while(cont_choice.equalsIgnoreCase("Y"));
	
	
	
	
	//obj1.search();
	//obj1.getALLfilefromDirectory();
	   
	   
	//obj1.getALLfilefromDirectorywithext();   //wait for this feature 
	//obj1.customfiles();
	
	//   obj1.Existenceoffiels();
	
//	obj1.deleteaspecficfile();
	   
	//   obj1.findfilewithconstraintonallattribute();
	   

	   
	   
	   
       /*
        * 
        * if (!hdfs.exists(new Path(metadatfilenameformat)))
        {
        	hdfs.create(new Path(metadatfilenameformat));
        }
        FSDataOutputStream fsout = hdfs.append(new Path(metadatfilenameformat));

		   // wrap the outputstream with a writer
		   PrintWriter writer = new PrintWriter(fsout);
		   writer.append(hdfs_target_ext + "/"+single_fn+"\n");
		   writer.close();
        */
    	  }
    	  
    	  
       private void searchwithspecificReq() {
	// TODO Auto-generated method stub
    	   
    	   System.out.println("Custom Search..put constraint while searching");
    	  
    	   Scanner sc=new Scanner(System.in);
    	   String Category = "NA";
 		 

   	  String year="NA"; 	       	      	 
   	   String month="NA";    
   	    String date="NA";
   	        	   
   	     String fileformat="NA";       	      	 
   	     String username="NA";    
   	    String size="NA";
   	        	   
   	    String timestmp="NA";
   	        	   
   	        	   
   	    String choice="NA";
   	  String cont="NA";
    	 
    	   do
    	   {
    		   System.out.println("1 : Enter Category for Constraining ");
    		   System.out.println("2 : Enter year to constraint on search Results ");
    		   System.out.println("3 : Enter month to constraint on search Results ");
    		   System.out.println("4 :Enter day to constraint on search Results ");
    		   System.out.println("5  :Enter fileformat to constraint on search Results ");
    		   System.out.println("6  :Enter username to constraint on search Results ");
    		   System.out.println("7  :Enter size of file to constraint on search Results ");
    		   System.out.println("8  :Enter timestamp when file is created in the format yyyy/mm/dd hh:mm:ss ");
    		   System.out.println("Enter your choice");
    		  choice = sc.next();
    		  if(choice.equals("1"))
    		  {
    			  Category = sc.next();
    		  }
    		  else if(choice.equals("2"))
    		  {
    			  year = sc.next();
    		  }
    		  else if(choice.equals("3"))
    		  {
    			  month = sc.next();
    		  }
    		  else if(choice.equals("4"))
    		  {
    			  date = sc.next();
    		  }
    		  else if(choice.equals("5"))
    		  {
    			  fileformat = sc.next();
    		  }
    		  else if(choice.equals("6"))
    		  {
    			  username = sc.next();
    		  }
    		  else if(choice.equals("7"))
    		  {
    			  size = sc.next();
    		  }
    		  else if(choice.equals("8"))
    		  {
    			  System.out.println("sdsdsds");
    			  timestmp= sc.nextLine();
    		  }
    		  
    		  
    		   
    		   
    		   
    		   System.out.println("Do you want to continue Y/N");
    		   cont = sc.next();
    		   
    	   }while(cont.equals("Y"));
    	   
    	   
    	   
    	   searchwithspecificReqDriver dd=new searchwithspecificReqDriver();
    	   try {
			dd.getfilewiththisconstraints(Category,year,month,date,fileformat,username,size,timestmp);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	   
    	   
    	 
    	   
    	   
    	   
	
}


	private void findfilewithconstraintonallattribute() {
	// TODO Auto-generated method stub
    	   
    	   System.out.println("Please enter the category of file");
    	   Scanner sc=new Scanner(System.in);
    	   
    	   String category = sc.next();
    	   
    	   System.out.println("Please enter the year of file creation");
    	   
    	   String year = sc.next();
    	   
        System.out.println("Please enter the month of file creation in number");
    	   
    	   String month = sc.next();
    	   
        System.out.println("Please enter the date of file creation in number");
    	   
    	   String date = sc.next();
 System.out.println("Please enter the format of file");
    	   
    	   String format = sc.next();
    	   String path="/lakes/"+category+"/"+year+"/"+month+"/"+date+"/"+format+"/";
    	   System.out.println(path);
    	   Configuration configuration = new Configuration();
	 	   try {
			FileSystem hdfs =  FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
			if(hdfs.exists(new Path(path)))
			{
				System.out.println("files at thi spath are as follow");
				FileStatus[] status=	hdfs.listStatus(new Path(path));
				for(FileStatus s:status)
				{
					System.out.println(s.getPath().getName().toString());
				}
			}
			else
			{
				System.out.println("no file occur at this path or the path is invalid");
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
		
	
}


	private void deleteaspecficfile() throws IOException, URISyntaxException {
	// TODO Auto-generated method stub
    	   
    	   System.out.println("Kindly enter the path of the file to be deleted");
    	   Scanner sc=new Scanner(System.in);
    	   String filewholepath = sc.next();
    	   
    	   Configuration configuration = new Configuration();
    	 	   FileSystem hdfs =  FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);;
			
    	 System.out.println("Status " + hdfs.exists(new Path(filewholepath.trim())));
    	
    	   try {
			if(hdfs.exists(new Path(filewholepath)))
			    {
				
			    	hdfs.delete(new Path(filewholepath));
			    	System.out.println("File deleted Successfully");
			    }
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
}


	private void Existenceoffiels() {
	// TODO Auto-generated method stub
    	   
    	   System.out.println("Enter the name of file or a subsequence whoes hdfs path you want to find");
    	   
    	   Scanner sc=new Scanner(System.in);
    	   String filename = sc.next();
    	   if(filename!=null && filename!="")
    	   {
    		   System.out.println(filename);
    		   ExistenceoffileDriver f =new ExistenceoffileDriver();
    		   try {
				f.searchfilePath(filename);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		   
    		   
    	   }
}


	private void getALLfilefromDirectorywithext() {
	// TODO Auto-generated method stub
    	   
    	   
    	   
    	System.out.println("Enter the directory");
   	
   		Scanner sc=new Scanner(System.in);
   		String inputPath=sc.next();
   		System.out.println(inputPath);
   		System.out.println("the extension to be searched");
   		String extension=sc.next();
   		if(inputPath.equals("1"))
   		{
   //	ViewAllfilefromDirectory(directoryPath);
   		}
    	   
    	   
    	   
    	   
    	   
    	   
	
}


	private void customfiles() {
	// TODO Auto-generated method stub
    	   
    	//   System.out.println("Custom Search by ");
	
}


	private void getALLfilefromDirectory() throws IOException, URISyntaxException {
    		System.out.println("Please select the operation you want to do");
    		System.out.println("enter 1 to View all fiels from a directory");
    		System.out.println("enter 2 to download all files from a directory");
    		Scanner sc=new Scanner(System.in);
    		String inputPath=sc.next();
    		System.out.println(inputPath);
    		System.out.println("Kindly enter the directory path to download");
    		String directoryPath=sc.next();
    		getfilebyExtension f =new getfilebyExtension();
    		//its remoing here .... when map reduce starts i need to give the extension too.....
    		
    		
    		
	
}


	public void ViewAllfilefromDirectory(String directoryPath) {
		// TODO Auto-generated method stub
		getfilebyPath f =new getfilebyPath();
		try {
			f.ViewAllfromdirectory(directoryPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	private void search() throws Exception {
	// TODO Auto-generated method stub
    	   
    	  searchbycategory();
    	 //  searchbyfileformat();
	
}


	private void searchbyfileformat() throws IOException, URISyntaxException {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
		
	}


	private void searchbycategory() throws Exception {
		// TODO Auto-generated method stub
		Configuration configuration = new Configuration();
		FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
		
		String cat_file_path ="/metadata/category.txt";
	    boolean cat_file = hdfs.exists(new Path("/metadata/category.txt"));
	    if(!cat_file)
	    {
	 	   hdfs.createNewFile(new Path(cat_file_path));
	    }
		
		
		
		
	   String hdfsmeta = ("/metadata/category.txt");
		String localOutputPath = "/home/akanshahduser/Desktop/lakes/Preprocessing/a1.txt";
		File file = new File(localOutputPath);
 	    if(file.exists())
 	    {
 	    	file.delete();
 	    	
 	    	
 	    }
		
		hdfs.copyToLocalFile(false, new Path(hdfsmeta), new Path(localOutputPath),true);
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
	Scanner sc = new Scanner(System.in);
		int choice = sc.nextInt();
		getfilebycategory_driver g = new getfilebycategory_driver();
		System.out.println("the choice you selected is " + a.get(choice));
		g.findfiles(a.get(choice));
		
	}


	private void Automatictagging() throws Exception {
	// TODO Auto-generated method stub
    	   Configuration configuration = new Configuration();
    	   Scanner sc=new Scanner(System.in);
    	   System.out.println("kindly enter the path of file or folder you want to update");
    	   FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
    	   String destinatn = "/top1";
    	   if(!hdfs.exists(new Path(destinatn)))
    	    {
    	    	 hdfs.mkdirs(new Path(destinatn));
    	    }
    	   String outputtopn = "/outputtopn1";
    	   if(hdfs.exists(new Path(outputtopn)))
    	    {
    	    	hdfs.delete(new Path(outputtopn));
    	    }
    	   /*
    	    * 
    	    * 
    	    * Scanner sc=new Scanner(System.in)
    	    * String spath = sc.next();
    	    */
    	   
    	   System.out.println("kindly enter the path of file or folder load files from");
    	   
    	  // String spath = "/home/akanshahduser/Desktop/lakes/files";
    	   String spath = sc.next();
    	   File file = new File(spath);
    	   if(file.exists())
    	   {
    	   if (file.isDirectory())
    	   {
    		   File[] listOfFiles =   file.listFiles();
    		   for(File single_fn:listOfFiles)
    		   {
    			   bestwords = new ArrayList();
    			   if(single_fn.getName().contains(".txt"))
    			   {
    				   bestwords  =  TopN(hdfs,single_fn.toString(),destinatn,outputtopn,single_fn);
    			   }
    			   else
    			   {
    				   String sdw= "Yettodecide";
    				   bestwords.add(sdw);
    			   }
    			   if (bestwords.size()>0)
    			   {
    			   filewritetohdfs.putAutomatedtaggeddatatohdfs(spath ,bestwords,single_fn.getName());
    			   }
    		   }
    		   //for each file call the word count giveing this path as parameter and get the top word
    		   //this will act as a category and rest is the same
    	   }
    	   else
    	   {
    		   
    		  ArrayList<String> bestwords =  TopN(hdfs,spath,destinatn,outputtopn,file);
    		   //Call word count
    		  if (bestwords.size()>0)
			   {
    			  System.out.println();;
			  filewritetohdfs.putAutomatedtaggeddatatohdfs(spath,bestwords,file.getName());
			   }
    	   }
    	   }
    	   else
    	   {
    		   System.out.println("file path does not exist");
    	   }
    	   
	
}
       
       
       public ArrayList<String> TopN(FileSystem hdfs, String spath, String destinatn, String outputtopn, File file) throws Exception
       {
    	   ArrayList<String> a =new ArrayList<String>();
    	   WordCount wc = new WordCount();
		   hdfs.copyFromLocalFile(new Path(spath), new Path(destinatn + "/"));
    	   String[] in_out = new String[]{destinatn + "/" + file.getName(), outputtopn};
		   wc.wordcount(in_out);
		   String cat_file_path = outputtopn+"/"+"part-00000";
   	    boolean cat_file = hdfs.exists(new Path(cat_file_path));
   	    if(cat_file)
   	    {
   	    	String localOutputPath = "/home/akanshahduser/Desktop/lakes/Preprocessing/topn/a1.txt";
   	    	hdfs.copyToLocalFile(new Path(cat_file_path), new Path("/home/akanshahduser/Desktop/lakes/Preprocessing/topn/a1.txt"));
   	     BufferedReader bf =new BufferedReader(new FileReader(new File(localOutputPath)));
         String s=null;
     
         int i=0;
        
        if((s=bf.readLine())!=null){
        	String[] n =s.split("\t");
             a.add(n[0]);
             System.out.println(s+"\t"+i);
             i=i+1;
         }
        
        File file1 = new File(localOutputPath);
 	    if(file1.exists())
 	    {
 	    	file1.delete();
 	    	
 	    	
 	    }
        
   	    }
		   //Write the logic to get the top 5 words and then writen them
		   
		   //deleting file after it is used
		   hdfs.delete(new Path(destinatn + "/" + file.getName()));
		   hdfs.delete(new Path(outputtopn)); 
		   
		   //dummy value
		//   a.add("ball");
		   return a;
       }
       
       
       public static void putAutomatedtaggeddatatohdfs(String path_location,ArrayList bestwords, String string) throws IllegalArgumentException, IOException, URISyntaxException
       {
    	   System.out.println("kindly write the path to where the mapping is there");
    	   
    	      String path_mapping="/home/abc/Desktop/dw/aa.txt";//=sc.next();
    	      System.out.println("kindly location of folder of file");
    	      Configuration configuration = new Configuration();
    	   // String path_location;//=sc.next();
    	  
    	   
    	   //you need to remove this path
    	      //path_location="/home/abc/Desktop/dw/data";
    	      int year = Calendar.getInstance().get(Calendar.YEAR);
    	      int month = Calendar.getInstance().get(Calendar.MONTH);
    	      int datetdy = Calendar.getInstance().get(Calendar.DATE);
    	      FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
    	      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	      Date date = new Date();
    	      System.out.println(dateFormat.format(date)); 
    	      if(!hdfs.exists(new Path("/lakes")))
    	      {
    	      	 hdfs.mkdirs(new Path("/lakes"));
    	      }
    	      
    	     
    	    String hdfs_path = "/lakes/";
    	      //creating metadata file
    	    if(!hdfs.exists(new Path("/metadata")))
    	    {
    	    	 hdfs.mkdirs(new Path("/metadata"));
    	    }
    	    String cat_file_path = "/metadata/category.txt";
    	    boolean cat_file = hdfs.exists(new Path("/metadata/category.txt"));
    	    if(!cat_file)
    	    {
    	 	   hdfs.createNewFile(new Path(cat_file_path));
    	    }
    	    //reading what user has given
    	    ArrayList<String> sd = new ArrayList<String>();
    	      Map<String,ArrayList<String>> category_path = new HashMap<String, ArrayList<String>>();
    	     
    	      sd.add(string);
    	      category_path.put(bestwords.get(0).toString(), sd);
    	      for(Map.Entry<String, ArrayList<String>> entry :  category_path.entrySet())
    	      {
    	    	  System.out.println(entry.getKey());
    	    	
    	    	  ArrayList<String> filename = entry.getValue();
    	    	  String hdfs_target = hdfs_path+entry.getKey()+"/"+year+"/"+month+"/"+datetdy;
    	 		  String metadatfilename = "/metadata/"+entry.getKey() + "_" + year + "_"+month; 
    	 		 FSDataOutputStream fsout1 = hdfs.append(new Path(cat_file_path));

      		   // wrap the outputstream with a writer
      		   PrintWriter writer1 = new PrintWriter(fsout1);
      		   writer1.append(entry.getKey()+"\n");
      		   writer1.close();
    	 		  
    	 		  
    	    	  for(String single_fn:filename)
    	    	  {
    	    		String extension =   single_fn.substring(single_fn.lastIndexOf(".") + 1).trim();
    	    		String hdfs_target_ext = hdfs_target + "/" + extension;
    	    		boolean bb =hdfs.isDirectory(new Path(hdfs_target_ext));
    	    		 if (bb==false)
    	    		 {
    	    			 hdfs.mkdirs(new Path(hdfs_target_ext));
    	    		 } 
    	    		 
    	    		System.out.println("Directory status " + bb);
    	    		//copying file into it
    	    		
    	    		
    	    		
    	      	   File file = new File(path_location);
    	      	  
    	      	   if (file.isDirectory())
    	      	   {
    	        hdfs.copyFromLocalFile(new Path(path_location+"/"+single_fn), new Path(hdfs_target_ext + "/"));
    	      	   }
    	      	   else
    	      	   {
    	      		 hdfs.copyFromLocalFile(new Path(path_location), new Path(hdfs_target_ext + "/"));
    	    	      	 
    	      	   }
    	        String metadatfilenameformat = metadatfilename  + ".txt";
    	       System.out.println(metadatfilenameformat);
    	       boolean exists = hdfs.exists(new Path(metadatfilenameformat));
    	       if(!exists)
    	       {
    	    	   hdfs.createNewFile(new Path(metadatfilenameformat));
    	       }
    	       System.out.println(exists);
    	       FSDataOutputStream fsout = hdfs.append(new Path(metadatfilenameformat));

    		   // wrap the outputstream with a writer
    		   PrintWriter writer = new PrintWriter(fsout);
    		   writer.append(extension + "\t" + hdfs_target_ext + "/"+single_fn+"###"+dateFormat.format(date)+"###"+new File(path_location+"/"+single_fn).length()+"###"+System.getProperty("user.name")+"###"+"rwx###rwx"+"\n");
    		   writer.close();
    		   
    	}

    	}
    	}
       


	//   System.out.println(f_p);
      
      
      
     

    		 
   




public void UpoadFiletoHDFS() throws IOException, URISyntaxException
{
	MapCategoryFile m = new MapCategoryFile();
    Scanner sc=new Scanner(System.in);
      System.out.println("kindly write the path to where the mapping is there");
      String path_mapping=sc.next();
    //  String path_mapping="/home/akanshahduser/Desktop/lakes/a1.txt";//=sc.next();
      System.out.println("kindly location of folder of file");
      Configuration configuration = new Configuration();
    String path_location;//=sc.next();
    path_location=sc.next();
   
   //   path_location="/home/akanshahduser/Desktop/lakes/files";
      int year = Calendar.getInstance().get(Calendar.YEAR);
      int month = Calendar.getInstance().get(Calendar.MONTH);
      int datetdy = Calendar.getInstance().get(Calendar.DATE);
      FileSystem hdfs = FileSystem.get(new URI("hdfs://172.50.88.17:54310"), configuration);
      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      Date date = new Date();
      System.out.println(dateFormat.format(date)); 
    
      if(!hdfs.exists(new Path("/lakes")))
      {
      	 hdfs.mkdirs(new Path("/lakes"));
      }
      
      //creating metadata file
    if(!hdfs.exists(new Path("/metadata")))
    {
    	 hdfs.mkdirs(new Path("/metadata"));
    }
    String cat_file_path ="/metadata/category.txt";
    boolean cat_file = hdfs.exists(new Path("/metadata/category.txt"));
    if(!cat_file)
    {
 	   hdfs.createNewFile(new Path(cat_file_path));
    }
    String hdfs_path = "/lakes/";
    
    //reading what user has given
      Map<String,ArrayList<String>> category_path = m.readfiles(path_mapping);
     
      for(Map.Entry<String, ArrayList<String>> entry :  category_path.entrySet())
      {
    	  System.out.println(entry.getKey());
    	
    	  ArrayList<String> filename = entry.getValue();
    	  String hdfs_target = hdfs_path+entry.getKey()+"/"+year+"/"+month+"/"+datetdy;
 		  String metadatfilename = "/metadata/"+entry.getKey() + "_" + year + "_"+month; 
 		  
 		  
 		 FSDataOutputStream fsout1 = hdfs.append(new Path(cat_file_path));

		   // wrap the outputstream with a writer
		   PrintWriter writer1 = new PrintWriter(fsout1);
		   writer1.append(entry.getKey()+"\n");
		   writer1.close();
 		  
 		  
 		  
    	  for(String single_fn:filename)
    	  {
    		String extension =   single_fn.substring(single_fn.lastIndexOf(".") + 1).trim();
    		String hdfs_target_ext = hdfs_target + "/" + extension;
    		boolean bb =hdfs.isDirectory(new Path(hdfs_target_ext));
    		 if (bb==false)
    		 {
    			 hdfs.mkdirs(new Path(hdfs_target_ext));
    		 } 
    		 
    		System.out.println("Directory status " + bb);
    		
    		
    		 File file = new File(path_location);
	      	  
	      	   if (file.isDirectory())
	      	   {
	        hdfs.copyFromLocalFile(new Path(path_location+"/"+single_fn), new Path(hdfs_target_ext + "/"));
	      	   }
	      	   else
	      	   {
	      		 hdfs.copyFromLocalFile(new Path(path_location), new Path(hdfs_target_ext + "/"));
	    	      	 
	      	   }
    		//copying file into it
       
        String metadatfilenameformat = metadatfilename  + ".txt";
       System.out.println(metadatfilenameformat);
       boolean exists = hdfs.exists(new Path(metadatfilenameformat));
       if(!exists)
       {
    	   hdfs.createNewFile(new Path(metadatfilenameformat));
       }
       System.out.println(exists);
       FSDataOutputStream fsout = hdfs.append(new Path(metadatfilenameformat));

	   // wrap the outputstream with a writer
	   PrintWriter writer = new PrintWriter(fsout);
	   writer.append(extension + "\t" + hdfs_target_ext + "/"+single_fn+"###"+dateFormat.format(date)+"###"+new File(path_location+"/"+single_fn).length()+"###"+System.getProperty("user.name")+"###"+"rwx###rwx"+"\n");
	   writer.close();
	   
}

}
}
}