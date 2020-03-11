package com.virtualpairprogrammers.keywordranking;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.virtualpairprogrammers.Util;

import scala.Tuple2;

public class top10InterestingWordsByCountInSubtitles {

	
	public void calculateTop10InterestingWords() {
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd =  sc.textFile("src/main/resources/subtitles/input-spring.txt");
		
		JavaRDD<String> lettersOnlyRDD = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		JavaRDD<String> removedBlankLines = lettersOnlyRDD.filter(sentence -> sentence.trim().length() > 0);
		
		JavaRDD<String> justWords = removedBlankLines.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());
		
		JavaRDD<String> justInterestingWords = justWords.filter(word -> word.length() > 0 && Util.isNotBoring(word));
		
		JavaPairRDD<String, Long> javaPairRdd = justInterestingWords.mapToPair(word -> new Tuple2<>(word, 1L));
		
		JavaPairRDD<String, Long> totals = javaPairRdd.reduceByKey((v1, v2) -> v1 + v2);
		
		JavaPairRDD<Long, String> switched = totals.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));
		
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
		
		sorted.take(10).forEach(System.out:: println);
		
		sc.close();
	}
	
}
