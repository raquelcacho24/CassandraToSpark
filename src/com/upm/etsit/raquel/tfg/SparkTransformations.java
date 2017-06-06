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
		/*String path = "total";
		String path1 = "manchester";
		String path2 = "football";
		String path3 = "score";
		String path4 = "resultado";
		String path5 = "resultado2";*/
		String path = "resultado";





		CassandraJavaRDD<CassandraRow> cassandraRDD = CassandraJavaUtil.javaFunctions(jscc).cassandraTable(keySpaceName, tableName);

		JavaRDD<Tweet> userRDD = cassandraRDD.map(new Function<CassandraRow, Tweet>() {

			public Tweet call(CassandraRow row) throws Exception {

				Tweet tweet = new Tweet(row.getLong("id"), row.getDate("date"),row.getString("name"), row.getString("text"),row.getInt("retweets"),row.getString("country"), row.getString("hashtags"));
				return tweet;
			}
		});

		JavaPairRDD<Long,List<String>> datos = userRDD.mapToPair(new PairFunction<Tweet,Long,List<String>>(){

			public Tuple2<Long, List<String>> call(Tweet t){
				List<String> datos = new ArrayList<String>();
				datos.add(t.getDate().toString());
				datos.add(t.getText().replaceAll(",", " ").replaceAll("\n\r"," ").replaceAll("\n"," "));
				datos.add(t.getName().replaceAll(",", " ").replaceAll("\n\r"," ").replaceAll("\n"," "));
				datos.add(t.getCountry());
				datos.add(t.getHashtags());


				return new Tuple2<Long,List<String>>(t.getId(),datos);
			}
		});

		// Número total de Tweets a analizar
		// (Nos servirá por si queremos hacer porcentajes, o ver que se están analizando el mismo nº de tweets en todos los RDDs)

		System.out.println("Total tweets"+ userRDD.count());
		


		/*userRDD.foreach(data -> {
            System.out.println(data.getText());    
            //logger.info(data._1()+"-"+data._2());
        });*/


		// Número de Tweets que contienen una sola palabra : por ejemplo, la palabra Manchester



		JavaPairRDD<Long,String> filtered_text_word = userRDD.mapToPair(new PairFunction<Tweet,Long,String>(){

			public Tuple2<Long, String> call(Tweet t){
				String resultado;
				if (t.getText().toLowerCase().contains("manchester") || t.getHashtags().toLowerCase().contains("manchester")){
					resultado="SI";
				}else{
					resultado="NO";
				}
				return new Tuple2<Long,String>(t.getId(),resultado);
			}
		});


		System.out.println("Manchester terrorism tweets Analyzed" + filtered_text_word.count());
		



		// Número de Tweets que hablan sobre un determinado tema: Por ejemplo fútbol

		String[] football_words = {"soccer", "football", "cristiano", "messi","champions","bench","championship","goal","goalkeeper","derby","penalty","league","Leicester","bayern","juventus","FCB","Real Madrid"};

		JavaPairRDD<Long,String> filtered_bytopic = userRDD.mapToPair(new PairFunction<Tweet,Long,String>(){
			public Tuple2<Long, String> call(Tweet t){
				String resultado;

				if (stringContainsItemFromList(t.getText().toLowerCase(),football_words) || stringContainsItemFromList(t.getHashtags().toLowerCase(),football_words)) {
					resultado="SI";
				} else {
					resultado="NO";
				}

				return new Tuple2<Long,String>(t.getId(),resultado);
			}
		});


		System.out.println("Football tweets analyzed"+ filtered_bytopic.count());

		// 	Asignación de puntuación negativo/positivo del sentimiento del texto de un Tweet

		List<String> positivas = loadPositiveWords();
		List<String> negativas = loadNegativeWords();


		/*for(int i = 0; i < negativas.size(); i++) {
            System.out.println(negativas.get(i));
        }
		 */

		//	Implementación haciendo reduce con el id_tweet --> genera tabla id_tweet | score


		JavaPairRDD<Long, Double> scored_tweets = userRDD.flatMap(new FlatMapFunction<Tweet, Tuple2<Long,String>>() {

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


		System.out.println("Scored tweets Analyzed"+ scored_tweets.count());


		//	Misma implementación pero haciendo reduce con el text_tweet--> genera tabla text_tweet | score
		// Implementación solo para ver los resultados, no debe ser la que genere una tabla para Tableu

		/*JavaPairRDD<String, Double> text = userRDD.flatMap(new FlatMapFunction<Tweet, Tuple2<String,String>>() {

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

		 */
		
		JavaPairRDD<Long, Tuple2<List<String>, String>> resultado1 = datos.join(filtered_bytopic);
		
		resultado1.foreach(data -> {
			System.out.println(data);;
		});
		
		
		resultado1.map(    x -> x._1 + ","  +   (  (x._2.toString()).replaceAll("[\\[\\]]", "").replaceAll("((\\)))", ""))   ).repartition(1);

		JavaPairRDD<Long, Tuple2<Tuple2<List<String>, String>, String>> resultado2 = resultado1.join(filtered_text_word);
		resultado2.map(    x -> x._1 + ","  +   (  (x._2.toString()).replaceAll("[\\[\\]]", "").replaceAll("((\\)))", ""))   ).repartition(1);
		
		JavaPairRDD<Long, Tuple2<Tuple2<Tuple2<List<String>, String>, String>, Double>> resultado3 = resultado2.join(scored_tweets);
		resultado3.map(    x -> x._1 + ","  +   (  (x._2.toString()).replaceAll("[\\[\\]]", "").replaceAll("((\\)))", ""))   ).repartition(1).saveAsTextFile(path);


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

		String csvFile = "positive-words.txt";
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

		String csvFile = "negative-words.txt";
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





