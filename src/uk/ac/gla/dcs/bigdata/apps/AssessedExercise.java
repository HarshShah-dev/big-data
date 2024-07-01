package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.CalculateNewsMetadata;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocRankGroupedReducer;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentRankingMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.FilterNewsArticles;
import uk.ac.gla.dcs.bigdata.studentfunctions.FrequencyReducer;
import uk.ac.gla.dcs.bigdata.studentfunctions.GrouperForDocRanking;
import uk.ac.gla.dcs.bigdata.studentfunctions.TupleToDocRankMap;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;



/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	
	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[2]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles
		String newsFile = System.getenv("bigdata.news");
		// if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json"; // Full 5gb dataset

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) outDirectory.mkdir();
			
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
		
		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here
		//----------------------------------------------------------------

		// Defining Accumulators to calcuate total no.of Documents in corpus and the sum of all document lengths
		LongAccumulator totalDocsInCorpus = spark.sparkContext().longAccumulator();
		LongAccumulator sumOfDocLengths = spark.sparkContext().longAccumulator();

		// Creating a query list and broadcasting it for better efficiency
		List<Query> queryList = queries.collectAsList();
		Broadcast<List<Query>> queryBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);
	
		// Applying a flatmap function to remove all the articles that do not contain any of the query terms
		// before beginning the other tasks, this also calculates total no.of Documents in corpus and 
		// the sum of all document lengths using the accumulators.
		Dataset<NewsArticle> filteredArticles = news.flatMap(new FilterNewsArticles(queryBroadcast, totalDocsInCorpus, sumOfDocLengths), Encoders.bean(NewsArticle.class));

		// Applying a flatmap function to calculate staistics(like term frequency in corpus) required to 
		// calculate DPH Score.
		// Returns the data in a TermFrequency wrapper class which implements a HashMap.
		Dataset<TermFrequency> termFrequency = filteredArticles.flatMap(new CalculateNewsMetadata(queryBroadcast), Encoders.bean(TermFrequency.class));
		TermFrequency combinedFrequency = termFrequency.reduce(new FrequencyReducer());

		// Calculating the averageDocumentLengthInCorpus using the above accumulator values.
		long totalDocsInCorpusValue = totalDocsInCorpus.value();
		double averageDocumentLengthInCorpus = sumOfDocLengths.value()/totalDocsInCorpus.value();


		// Printing few statistics to see the result after the CalculateNewsMetadata FlatMapFunction
		System.out.println("Printing: ");
		System.out.println(combinedFrequency);
		System.out.println(totalDocsInCorpusValue);
		System.out.println(sumOfDocLengths.value());
		System.out.println(averageDocumentLengthInCorpus);		

		// Applying a flatmap to calculate the score for each NewsArticle for each Query.
		Dataset<DocumentRanking> documentRankings = filteredArticles.flatMap(new DocumentRankingMap(queryBroadcast, combinedFrequency, totalDocsInCorpusValue, averageDocumentLengthInCorpus), 
		Encoders.bean(DocumentRanking.class));

		// Grouping the Document Rankings taking the Query as the key.
		GrouperForDocRanking keyFunction = new GrouperForDocRanking();
		KeyValueGroupedDataset <Query, DocumentRanking> documentRankingsGrouped = documentRankings.groupByKey(keyFunction, Encoders.bean(Query.class));

		// Using reduceGroups to merge all DocumentRankings that have the same Query while also removing the 
		// similar titles using the TextDistancCalculator, returning the top 10 RankedResults for each DocumentRanking. 
		Dataset<Tuple2<Query, DocumentRanking>> tupleQueryDocRank = documentRankingsGrouped.reduceGroups(new DocRankGroupedReducer());
		
		// Mapping function to Map from Tuple to DocumentRanking
		Dataset<DocumentRanking> groupedDocRankings = tupleQueryDocRank.map(new TupleToDocRankMap(), Encoders.bean(DocumentRanking.class));

		// Collecting the DocumentRankings as a list, this invokes the above tasks.
		List<DocumentRanking> listOfDocRank = groupedDocRankings.collectAsList();
		System.out.println(listOfDocRank);
		
		return listOfDocRank; // replace this with the the list of DocumentRanking output by your topology
	}
	
}
