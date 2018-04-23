package usertag;
import java.util.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;

public class VideoReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
	  
	    String country1= context.getConfiguration().get("country1");
	    String country2= context.getConfiguration().get("country2");

        
		ArrayList<String>  country1_video = new ArrayList<String>();
		ArrayList<String>  country2_video = new ArrayList<String>();
		Set<String> hs = new HashSet<>();
		
		double sum1=0;
		double sum2=0;
		for (Text text: values){
			String  country_and_video= text.toString();
			String[] c_v = country_and_video.split("/"); //split the values so we can get country and videoID
			if(c_v[0].equals(country1)){
			     
			    hs.add(c_v[1]); 			//put the videoID of country1 into hashset to eliminate duplicates 
			}
			else if (c_v[0].equals(country2)){
			   country2_video.add(c_v[1]);		//put the videoID of country2 in an arraylist
				 
			}
			else{return;}
				
		}
		
		
		country1_video.addAll(hs);
		sum1=country1_video.size();

	  	//find the number of same videos in both country1 and country2
		for(int i=0;i<sum1;i++){
		    for(int j=0;j<country2_video.size();j++){
			if (country1_video.get(i).equals(country2_video.get(j))){
			    sum2++;
			    break; 				//when a match is found, break the loop
			}
		    }
		}
		double percent; 
		if(sum1==0){
		    percent=0;
		}
		else{
		    percent = sum2/sum1*100;}
		String percentage = String.format("%.2f",percent);
		String number =("total: "+String.valueOf(sum1)+"; "+percentage+"% in "+country2);
	
		result.set(number);
		context.write(key, result);
	}
}
