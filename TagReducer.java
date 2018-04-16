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

/**
 * Input record format
 * dog -> {(48889082718@N01=1,48889082718@N01=3,), 423249@N01=4,}
 *
 * Output for the above input key valueList
 * dog -> 48889082718@N01=4,3423249@N01=4,
 * 
 * @see TagSmartMapper
 * @see TagSmartDriver
 * 
 * @author Ying Zhou
 *
 */
public class TagReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
	  
	    String country1= context.getConfiguration().get("country1");
	    String country2= context.getConfiguration().get("country2");

        
		ArrayList<String>  country1_video = new ArrayList<String>();
		ArrayList<String>  country2_video = new ArrayList<String>();
		double sum1=0;
		double sum2=0;
		for (Text text: values){
			String  country_and_video= text.toString();
			String[] c_v = country_and_video.split("/");
			if(c_v[0].equals(country1)){
			    country1_video.add(c_v[1]);   
			}
			else if (c_v[0].equals(country2)){
			    country2_video.add(c_v[1]);
			}
			else{return;}
				
		}
		
		Set<String> hs = new HashSet<>();
		hs.addAll(country1_video);
		country1_video.clear();
		country1_video.addAll(hs);
		sum1=country1_video.size();
	  
		for(int i=0;i<sum1;i++){
		    for(int j=0;j<country2_video.size();j++){
			if (country1_video.get(i).equals(country2_video.get(j))){
			    sum2++;
			    break;
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
