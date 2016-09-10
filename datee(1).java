

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class datee {
public static void main(String args[])
{
	int year = Calendar.getInstance().get(Calendar.YEAR);
	System.out.println(year);
	DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
	Date date = new Date();
	System.out.println(dateFormat.format(date)); //2014/08/06 15:59:48
	
	/*
	String s = "sdsd.dsdsd.a.aa";
	System.out.println(s.substring(s.lastIndexOf(".") + 1).trim());
	 String spath = "/home/abc/Desktop/dw/data";
	   File file = new File(spath);
	   File[] listOfFiles =   file.listFiles();
	   for(File single_fn:listOfFiles)
	   {
	   System.out.println(single_fn);
	   }
	   */
	 String  ss="ball_2016_3.txt";
	 String choicecat ="ball";
	 System.out.println(ss);
	 System.out.println(choicecat);
	 boolean t =ss.contains(choicecat);
	   System.out.println(t);
	   
	   int datetdy = Calendar.getInstance().get(Calendar.DATE);
	   System.out.println(datetdy);
	   String st= "2015_3_cricke";
	   String dateee="2015_3_crciket";
	   if(dateee.contains(st))
	   {
		   System.out.println("ff");
	   }
}
}
