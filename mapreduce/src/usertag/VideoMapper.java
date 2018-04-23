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

public class VideoMapper extends Mapper<Object, Text, Text, Text> {
    private Text category = new Text(),country = new Text();
	static CharsetEncoder asciiEncoder = Charset.forName("US-ASCII").newEncoder(); 
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
	  
	    String country1= context.getConfiguration().get("country1");
	    String country2= context.getConfiguration().get("country2");
	    String[] dataArray = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //split the data into array column by column
		
			if(dataArray[17].trim().equals(country1)||dataArray[17].trim().equals(country2)){
			category.set(dataArray[5].trim());				//set the 6th column as category
			country.set(dataArray[17].trim()+"/"+dataArray[0].trim()); 	//set the "country name / videoID" as country 
			context.write(category,country);
		    }
		 
		
		  
	       
	}
}
