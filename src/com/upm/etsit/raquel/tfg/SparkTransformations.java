package com.upm.etsit.raquel.tfg;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
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

				Tweet tweet = new Tweet(row.getLong("id"), row.getDate("date"),row.getString("name"), row.getString("text"),row.getInt("retweets"),row.getString("country"));
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

		// 	Asignación de puntuación negativo/positivo del sentimiento del texto de un Tweet

		//	Primero implementación haciendo reduce con el id_tweet --> genera tabla id_tweet | score


		/*JavaPairRDD<Long, Double> text = userRDD.flatMap(new FlatMapFunction<Tweet, Tuple2<Long,String>>() {

			@Override
			public Iterator<Tuple2<Long,String>> call(Tweet t) {
				List<Tuple2<Long,String>> list = new ArrayList<Tuple2<Long,String>>();
				String[] words = t.getText().split(" ");
				for(int i=0;i<words.length;i++)
					list.add(new Tuple2<Long,String>(t.getId(),words[i]));
				return list.iterator();
			}
		}).mapToPair(new PairFunction<Tuple2<Long,String>,Long,Double>(){

			@Override
			public Tuple2<Long, Double> call(Tuple2<Long, String> arg0){
				return new Tuple2<Long,Double>(arg0._1,getScore(arg0._2, positivas, negativas));
			}

		}).groupByKey().

				mapToPair(new PairFunction<Tuple2<Long,Iterable<Double>>,Long,Double>(){

					@Override
					public Tuple2<Long, Double> call(Tuple2<Long, Iterable<Double>> arg0) throws Exception {
						Iterator<Double> it = arg0._2.iterator();
						double final_score = 0.0;
						int i=0;
						while(it.hasNext()){
							final_score+= it.next();
							i++;
						}
						return new Tuple2<Long,Double>(arg0._1,final_score/i);
					}

				});


		text.foreach(data -> {
			SparkTransformations.showOutput(data);
		});*/

		//	Misma implementación pero haciendo reduce con el text_tweet--> genera tabla text_tweet | score
		// Implementación solo para ver los resultados, no debe ser la que genere una tabla para QlikView

		JavaPairRDD<String, Double> text = userRDD.flatMap(new FlatMapFunction<Tweet, Tuple2<String,String>>() {

			@Override
			public Iterator<Tuple2<String,String>> call(Tweet t) {
				List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();
				String[] words = t.getText().split(" ");
				for(int i=0;i<words.length;i++)
					list.add(new Tuple2<String,String>(t.getText(),words[i]));
				return list.iterator();
			}
		}).mapToPair(new PairFunction<Tuple2<String,String>,String,Double>(){

			@Override
			public Tuple2<String, Double> call(Tuple2<String, String> arg0){
				return new Tuple2<String,Double>(arg0._1,getScore(arg0._2, positivas, negativas));
			}

		}).groupByKey().

				mapToPair(new PairFunction<Tuple2<String,Iterable<Double>>,String,Double>(){

					@Override
					public Tuple2<String, Double> call(Tuple2<String, Iterable<Double>> arg0) throws Exception {
						Iterator<Double> it = arg0._2.iterator();
						double final_score = 0.0;
						int i=0;
						while(it.hasNext()){
							final_score+= it.next();
							i++;
						}
						return new Tuple2<String,Double>(arg0._1,final_score/i);
					}

				});


		text.foreach(data -> {
			SparkTransformations.showOutput(data);
		});





	}



	public static synchronized void showOutput(Tuple2<String,Double> data){
		System.out.println("Tweet: " + data._1 + " scored as: " + data._2);
	}

	// Metodo para saber si un texto es Negativo o Positivo

	public static Double getScore(String inputStr, List<String> positivo, List<String> negativo) {

		double score = 0;

		if ( positivo.contains(inputStr) ){
			score=score + 1.0;
		}
		else if ( negativo.contains(inputStr) ){
			score=score - 1.0;
		}


		return score;


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





