package ml;

import java.util.*;


import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import java.io.StringReader;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import com.clearspring.analytics.util.Lists;



public class VideoSort {

	public static void main(String[] args) {



        String inputDataPath = args[0],outputDataPath = args[1];
        SparkConf conf = new SparkConf();
        conf.setAppName("Video trending Application");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> ratingData = sc.textFile(inputDataPath);
        JavaPairRDD<String, String> videoExtraction = ratingData.mapToPair(s ->
                {     String[] dataArray = s.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); //split data column by column

                    return
                            new Tuple2<String, String>(dataArray[17]+"/"+dataArray[0],dataArray[8]);}
        );
        
        JavaPairRDD<String, Iterable<String>>genreRatingAvg = videoExtraction.groupByKey();

        Function<Tuple2<String,java.lang.Double>,Boolean> filterPredicate = e -> e._2>=1000; //create a filter that chooses the percentage that is >=1000

        JavaPairRDD<String, java.lang.Double> toptwoVideos = genreRatingAvg.flatMapToPair(s -> {
                    String id_country = s._1;
                    List <String> list_of_videos = Lists.newArrayList(s._2);

                    ArrayList<Tuple2<String, java.lang.Double>> results = new ArrayList<Tuple2<String, java.lang.Double>>();
         	   //find the first two trendings
                   double[] views=new double[2];
                   if(list_of_videos.size()>=2) {
                       for (int i = 0; i < list_of_videos.size(); i++) {
                            String mark=list_of_videos.get(i);
                            double view= java.lang.Double.parseDouble(mark);
                            views[i]=view;

                           if (i == 1) {

                               break;
                           }
                       }
                   }
		//calculate the percentage of increase
                 double ratiopercent=(views[1]-views[0])/views[0]*100;
                 results.add(new Tuple2<String, java.lang.Double>(id_country, ratiopercent));
                 return results.iterator();
                }
        ).filter(filterPredicate);
	//map the data into String format
        JavaRDD<String> TTV=toptwoVideos.map(s->{
            String[] CounId=s._1.split("/");
            String de=CounId[0]+" "+CounId[1]+" "+String.format("%.1f",s._2);
            return de;
        });
	//use the SecondSort method to define two keys
        JavaPairRDD<SecondSortKey,String> pairRDD=TTV.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            public Tuple2<SecondSortKey, String> call(String t) throws Exception {
                              String[] split = t.split(" ");
                               String first = split[0];
                                double tran=Double.parseDouble(split[2]);
                               Long second = new Double(tran).longValue();
                               SecondSortKey ssk = new SecondSortKey(first, second);
                               return new Tuple2<SecondSortKey, String>(ssk, t);
                           }
       });
	//sort the data using second sort key
        JavaPairRDD<SecondSortKey, String> sortByKeyRDD =pairRDD.sortByKey(false);
	//map the data into output format
        JavaRDD<String> mapRDD = sortByKeyRDD.map(new Function<Tuple2<SecondSortKey,String>, String>() {
            public String call(Tuple2<SecondSortKey, String> v1) throws Exception {

                                return v1._2.split(" ")[0]+"; "+v1._2.split(" ")[1]+", "+v1._2.split(" ")[2]+"%";
                           }
        });


        mapRDD.coalesce(1).saveAsTextFile(outputDataPath);
        sc.close();
    }
}
