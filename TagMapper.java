package usertag;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.Configuration;
/**
 * 
 * This does similar thing to TagMapper, except that
 * we make the map output value format exactly the same as
 * reduce output value format. So that we can use the reducer as combiner
 *   
 * input record format
 * 2048252769	48889082718@N01	dog francis lab	2007-11-19 17:49:49	RRBihiubApl0OjTtWA	16
 * 
 * output key value pairs for the above input
 * dog -> 48889082718@N01=1,
 * francis -> 48889082718@N01=1,
 * 
 * 
 * @see TagSmartReducer
 * @see TagSmartDriver
 * @author Ying Zhou
 *
 */
public class TagMapper extends Mapper<Object, Text, Text, Text> {
    private Text category = new Text(),country = new Text();
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
	  
	    String country1= context.getConfiguration().get("country1");
	    String country2= context.getConfiguration().get("country2");
	    String[] dataArray = value.toString().split(","); //split the data into array
		
		if (dataArray.length==18)
		    {
			if(dataArray[17].trim().equals(country1)||dataArray[17].trim().equals(country2)){
			category.set(dataArray[5].trim());
			country.set(dataArray[17].trim()+"/"+dataArray[0].trim());
			context.write(category,country);
		    }
		    }
		else if(dataArray.length > 18){
		    String[] largeArray = value.toString().split("000Z");
		    String[] frontArray = largeArray[0].split(",");
		     if(dataArray[dataArray.length-1].trim().equals(country1)||dataArray[dataArray.length-1].trim().equals(country2)){
			category.set(dataArray[frontArray.length-2].trim());
			country.set(dataArray[dataArray.length-1].trim()+"/"+dataArray[0].trim());
			context.write(category,country);
		    }
		     else{return;}

		}
        
		

	      
	
		  
	       
	}
}
