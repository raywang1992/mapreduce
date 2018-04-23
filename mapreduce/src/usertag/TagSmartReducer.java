package usertag;

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
public class TagSmartReducer extends Reducer<Text, Text, Text, Text> {
	Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {
	  
	    String country1= context.getConfiguration().get("country1");
	    String country2= context.getConfiguration().get("country2");

		// create a map to remember the owner frequency
		// keyed on owner id
		Map<String, Integer> vidoeFrequency = new HashMap<String,Integer>();
		ArrayList<String>  countryA = new ArrayList<String>();
		ArrayList<String>  countryB = new ArrayList<String>();
		int sumA=0;
		
		for (Text text: values){
			String  country_and_video= text.toString();
			String[] c_v = country_and_video.split("/");
			if(c_v[0].equals(country1)){
			    countryA.add(c_v[1]);
			    sumA=sumA+1;
			}
				
		}

		String number =String.valueOf(sumA);
	
		result.set(number);
		context.write(key, result);
	}
}
