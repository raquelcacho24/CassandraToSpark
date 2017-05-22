package com.upm.etsit.raquel.tfg;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

import scala.Tuple2;

public class SparkTransformations {

	public static void main(String args[]) throws InterruptedException{

		SparkConf conf = new SparkConf().setMaster("local[5]").setAppName("Spark-Cassandra Integration")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");


		conf.set("spark.cassandra.connection.host", "192.168.56.103");
		conf.set("spark.cassandra.connection.port", "9042");

		JavaSparkContext jscc = new JavaSparkContext(conf);

		String keySpaceName = "twitterkeyspace";
		String tableName = "tweets";


		CassandraJavaRDD<CassandraRow> cassandraRDD = CassandraJavaUtil.javaFunctions(jscc).cassandraTable(keySpaceName, tableName);

		JavaRDD<Tweet> userRDD = cassandraRDD.map(new Function<CassandraRow, Tweet>() {

			public Tweet call(CassandraRow row) throws Exception {
				Tweet tweet = new Tweet(row.getDate("date"),row.getString("name"), row.getString("text"),row.getInt("retweets"),row.getString("country"));
				return tweet;
			}
		});

		//Número total de Tweets a analizar (Nos servirá para luego hacer porcentajes)

		long total_tweets = userRDD.count();

		/*userRDD.foreach(data -> {
            System.out.println(data.getText());    
            //logger.info(data._1()+"-"+data._2());
        });*/


		// Número de Tweets que contienen una sola palabra : por ejemplo, la palabra España

		/*JavaRDD<String> filtered_text_palabra = userRDD.filter(new Function<Tweet, Boolean>() {

			public Boolean call(Tweet t) throws Exception {

				if (t.getText().contains("España")) {
					return true;
				} else {
					return false;
				}
			}
		}).map(new Function<Tweet, String>() {
			public String call(Tweet t) {

				return t.getText();
			}
		});

		filtered_text_palabra.foreach(data -> {
			System.out.println(data);    
		});

		System.out.println(filtered_text_palabra.count());
		 */


		// Número de Tweets que hablan sobre un determinado tema: Por ejemplo comida

		/*List<String> comer = Arrays.asList("cena", "comida", "hambre", "aproveche","food","sushi","hamburguesa");

		JavaRDD<String> filtered_text_comida = userRDD.filter(new Function<Tweet, Boolean>() {

			public Boolean call(Tweet t) throws Exception {

				if (stringContainsItemFromList(t.getText(),comer)) {
					return true;
				} else {
					return false;
				}
			}
		}).map(new Function<Tweet, String>() {
			public String call(Tweet t) {

				return t.getText();
			}
		});

		filtered_text_comida.foreach(data -> {
			System.out.println(data);    
		});

		System.out.println(filtered_text_comida.count());
		 */

		// Experimento

	

		List<String> positivas = loadPositiveWords();
		List<String> negativas = loadNegativeWords();
		
		
		/*for(int i = 0; i < negativas.size(); i++) {
            System.out.println(negativas.get(i));
        }
		*/
	

		JavaPairRDD<String, Integer> text = userRDD.mapToPair(new PairFunction<Tweet, String, Integer>() {
			public Tuple2<String, Integer> call(Tweet t) {

				int score = 0;
				score = getScore(t.getText(), positivas, negativas);		
				return new Tuple2<String, Integer>(t.getText(), score);
			}
		});

		text.foreach(data -> {
			System.out.println(data._2);    
		});

	}




	// Metodo para saber si un texto es Negativo o Positivo

	public static int getScore(String inputStr, List<String> positivo, List<String> negativo) {

		int score = 0;
		//int neutral_words = 0;
		int total_score = 0;

		List<String> words = Arrays.asList(inputStr.split(" "));

		for(int i=0; i<words.size() ; i++){
			for( int j=0; j< positivo.size() ; j++){
				if (  words.get(i).equals(positivo.get(j)) ){
					score=score+1;
				}
			}
		}
		
		for(int i=0; i<words.size() ; i++){
			for( int j=0; j< negativo.size() ; j++){
				if (  words.get(i).equals(negativo.get(j)) ){
					score=score-1;
				}
			}
		}

		total_score = score/words.size();
		
		return total_score;

		

	}

	// Método para saber si un texto contiene alguna lista de palabras

	public static boolean stringContainsItemFromList(String inputStr, String[] items) {
		return Arrays.stream(items).parallel().anyMatch(inputStr::contains);
	}

	public static List<String> loadPositiveWords(){

		String csvFile = "pos_excel.csv";
		BufferedReader br = null;
		String line = "";
		List<String> positivas = new ArrayList<String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				positivas.add(line.toLowerCase());
				
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return positivas;

	}
	
	public static List<String> loadNegativeWords(){

		String csvFile = "neg_excel.csv";
		BufferedReader br = null;
		String line = "";
		List<String> negativas = new ArrayList<String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				negativas.add(line.toLowerCase());
				
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		return negativas;

	}





}





