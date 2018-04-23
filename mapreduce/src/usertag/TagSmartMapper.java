package usertag;

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
public class TagSmartMapper extends Mapper<Object, Text, Text, Text> {
    private Text category = new Text(),country = new Text();
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
	  
	    String country1= context.getConfiguration().get("country1");
	    String country2= context.getConfiguration().get("country2");
		String[] dataArray = value.toString().split(","); //split the data into array
		if (dataArray.length == 18){ //  record with incomplete data
		    if(dataArray[17].trim().equals(country1)){
			category.set(dataArray[5].trim());
			country.set(dataArray[17].trim()+"/"+dataArray[0].trim());
			context.write(category,country);
		    }

		}
		else{return;}
	       
	}
}