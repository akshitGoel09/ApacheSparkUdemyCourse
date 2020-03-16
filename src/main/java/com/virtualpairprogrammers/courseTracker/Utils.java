package com.virtualpairprogrammers.courseTracker;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class Utils {

	public static void printJavaRDD(JavaRDD<?> javaRdd) {

		System.out.println();
		System.out.println();
		javaRdd.collect().forEach(System.out::println);
		System.out.println();
		System.out.println();
	}

	public static void printJavaRDD(JavaPairRDD<?, ?> javaRdd) {

		System.out.println();
		System.out.println();
		javaRdd.collect().forEach(System.out::println);
		System.out.println();
		System.out.println();
	}
	

}
