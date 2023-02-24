package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.ArticleContentToQueryTermCount;

import uk.ac.gla.dcs.bigdata.studentfunctions.ArticlesContentToRankedResultList;
import uk.ac.gla.dcs.bigdata.studentfunctions.CompleteRankedResultList;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticlePairing;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticlePartitioner;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleToArticleContent;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueriesToTotalQueryTerms;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryListUnifier;
import uk.ac.gla.dcs.bigdata.studentfunctions.QueryStringCompiler;
import uk.ac.gla.dcs.bigdata.studentfunctions.SumQueryTermCounts;
import uk.ac.gla.dcs.bigdata.studentfunctions.queryToQueryLists;

import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContent;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleContentToQueryTermCountArray;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryToQueryList;

import uk.ac.gla.dcs.bigdata.studentstructures.RankedResultList;
import uk.ac.gla.dcs.bigdata.studentstructures.TotalQueryTermsArray;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by the
 * spark.master environment variable.
 * 
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get
														// an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark
																			// finds it

		// The code submitted for the assessed exercise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef == null)
			sparkMasterDef = "local[2]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile == null)
			queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		if (newsFile == null)
			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default
		// is a sample of 5000 news articles
//			newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // 5GB test case

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results == null)
			System.err
					.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/" + System.currentTimeMillis());
			if (!outDirectory.exists())
				outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}

	}

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects

		// converts each row to a Query
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class));

		// converts each row to a NewsArticle
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class));

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		// define accumulators for total terms in corpus and total documents in corpus
		LongAccumulator termCountAccumulator = spark.sparkContext().longAccumulator();
		LongAccumulator articleCountAccumulator = spark.sparkContext().longAccumulator();

		// make a list of query using map reduce
		Dataset<QueryToQueryList> querytoQueryLists = queries.map(new queryToQueryLists(),
				Encoders.bean(QueryToQueryList.class));
		QueryToQueryList queryListResult = querytoQueryLists.reduce(new QueryListUnifier());
		List<Query> queryList = queryListResult.getQuerytoQueryList();

		// broadcast list of query terms
		Broadcast<List<Query>> broadcastQueryList = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(queryList);

		// make an array of query terms combining all queries
		Dataset<TotalQueryTermsArray> totalQueryTerms = queries.flatMap(new QueriesToTotalQueryTerms(),
				Encoders.bean(TotalQueryTermsArray.class));
		TotalQueryTermsArray compiledTotalQueryTerms = totalQueryTerms.reduce(new QueryStringCompiler());

		// broadcast array of string of query terms
		Broadcast<String[]> broadcastQueryTerms = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(compiledTotalQueryTerms.getTotalQueryTerms());

		// processes documents using provided processor, accumulates total term in
		// corpus and total document in corpus
		Dataset<ArticleContent> articlesContent = news.flatMap(
				new NewsArticleToArticleContent(termCountAccumulator, articleCountAccumulator, broadcastQueryTerms),
				Encoders.bean(ArticleContent.class));

		// partition articleContent to 2 partitions
		JavaRDD<ArticleContent> articlesContentAsRDD = articlesContent.toJavaRDD();
		NewsArticlePairing newsArticleExtractor = new NewsArticlePairing();
		JavaPairRDD<NewsArticle, ArticleContent> articlesContentWithNewsArticleKeyAsRDDPair = articlesContentAsRDD
				.mapToPair(newsArticleExtractor);
		JavaPairRDD<NewsArticle, ArticleContent> articlesContentRepartionedAsRDDPair = articlesContentWithNewsArticleKeyAsRDDPair
				.partitionBy(new NewsArticlePartitioner(2));
		JavaRDD<ArticleContent> articlesContentRepartionedAsRDD = articlesContentRepartionedAsRDDPair.map(x -> x._2);
		Dataset<ArticleContent> articlesContentRepartioned = spark.createDataset(articlesContentRepartionedAsRDD.rdd(),
				Encoders.bean(ArticleContent.class));

		// calculate occurrence of each query terms in corpus, stored as array of short
		// array of short has indexing corresponding to array of string of query terms
		// created earlier
		Dataset<ArticleContentToQueryTermCountArray> articleContentToQueryTermCount = articlesContentRepartioned
				.flatMap(new ArticleContentToQueryTermCount(),
						Encoders.bean(ArticleContentToQueryTermCountArray.class));
		ArticleContentToQueryTermCountArray totalQueryTermCounts = articleContentToQueryTermCount
				.reduce(new SumQueryTermCounts());

		// broadcast no. of occurrence of each query terms in corpus, total terms in
		// corpus, total document in corpus
		Broadcast<short[]> broadcastTotalQueryTermCounts = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(totalQueryTermCounts.getArticleContentToQueryTermCount());
		Broadcast<Long> broadcastTermCountAccumulator = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(termCountAccumulator.value());
		Broadcast<Long> broadcastArticleCountAccumulator = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(articleCountAccumulator.value());

		// define the return instance of a list of DocumentRanking
		List<DocumentRanking> documentRankingList = new ArrayList<DocumentRanking>();

		// iterate through each query
		for (int queryIndex = 0; queryIndex < queryList.size(); queryIndex++) {
			Query query = queryList.get(queryIndex);

			// Get the DPH result of query and each article using a flatMap
			Dataset<RankedResultList> rankedResultLists = articlesContentRepartioned
					.flatMap(new ArticlesContentToRankedResultList(queryIndex, broadcastQueryTerms, broadcastQueryList,
							broadcastTotalQueryTermCounts, broadcastTermCountAccumulator,
							broadcastArticleCountAccumulator), Encoders.bean(RankedResultList.class));

			// reduces the Dataset of RankedResultList into a list containing only
			// RankedResult with non-zero DPH values
			RankedResultList rankedResultList = rankedResultLists.reduce(new CompleteRankedResultList());
			List<RankedResult> rankedResults = rankedResultList.getRankedResultList();

			// Sorts the list of RankedResult in descending order based on DPH score
			Collections.sort(rankedResults, Collections.reverseOrder());

			// define return instance of List of RankedResult
			List<RankedResult> result = new ArrayList<RankedResult>();

			// create a list of RankedResult of top 10 rankings
			int index = 0;
			RankedResult newResult;
			double similarity = 0.0;
			boolean similar;
			while ((result.size() < 10) && (index < rankedResults.size())) {
				similar = false;
				newResult = rankedResults.get(index);

				// calculates the text distance with rankedResults that are already in the list
				if (!result.isEmpty()) {
					for (RankedResult current : result) {
						similarity = TextDistanceCalculator.similarity(current.getArticle().getTitle(),
								newResult.getArticle().getTitle());
						if (similarity < 0.5) {
							similar = true;
							break;
						}
					}
					if (!similar) {
						result.add(newResult);
					}
				} else {
					result.add(newResult);
				}
				index++;
			}

			// add DocumentRanking instance of query to the list of DocumentRanking
			DocumentRanking documentRanking = new DocumentRanking();
			documentRanking.setQuery(query);
			documentRanking.setResults(result);
			documentRankingList.add(documentRanking);
		}
		// replace this with the the list of DocumentRanking output by your topology
		return documentRankingList;
	}

}
