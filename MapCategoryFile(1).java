

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapCategoryFile {

	public static void main(String[] args) throws IOException {
		
		String filename="/home/akanshahduser/Desktop/aa.txt";//path is workspace
		
		Map<String,ArrayList<String>> catfilemap =readfiles(filename);
		System.out.println(catfilemap);

	}
	public static Map<String,ArrayList<String>> readfiles(String filename) throws IOException{
			
		Map<String,ArrayList<String>>  catfilemap=new LinkedHashMap<String,ArrayList<String>>();
		BufferedReader bf =new BufferedReader(new FileReader(new File(filename)));
		String s=null;
		
		while((s=bf.readLine())!=null){
			String [] toks=s.split("\t");
			String [] filearr=toks[1].split(",");
			ArrayList<String> temparr=new ArrayList<String>();
			for(String file: filearr){
				temparr.add(file);
			}
			catfilemap.put(toks[0], temparr);
		}
		
					
		
		
		return catfilemap;	
	}	

}
