package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;

// Calculates DPH score for each article based on each query.
public class DocumentRankingMap implements FlatMapFunction<NewsArticle, DocumentRanking> {

    Broadcast<List<Query>> queryBroadcast;
    TermFrequency combinedFrequency;
    long totalDocsInCorpusValue;
    double averageDocumentLengthInCorpus;

    private transient TextPreProcessor processor;
    
    public DocumentRankingMap(Broadcast<List<Query>> queryBroadcast, TermFrequency combinedFrequency, long totalDocsInCorpusValue, double averageDocumentLengthInCorpus) {
        this.queryBroadcast = queryBroadcast;
        this.combinedFrequency = combinedFrequency;
        this.totalDocsInCorpusValue = totalDocsInCorpusValue;
        this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
    }


    @Override
    public Iterator<DocumentRanking> call(NewsArticle value) {

        if (processor==null) processor = new TextPreProcessor();

        // Tokenizing Article
        List<String> contentTerms = new ArrayList<>();
        if(value.getTitle() != null){
            contentTerms.addAll(processor.process(value.getTitle()));
        }
        List<String> contentTerms2 = preProcessContent(value);
        contentTerms.addAll(contentTerms2);

        // List of DocumentRankings
        List<DocumentRanking> listOfDocumentRankings = new ArrayList<>();

        // Looping through each query and query term to calculate the DPH Score.
        for(Query query : queryBroadcast.value()){
            
            List<String> queryTerms = query.getQueryTerms();
            double score = 0;
            for(String queryTerm : queryTerms){

                short occurrences = (short) Collections.frequency(contentTerms, queryTerm);
                if (occurrences>0){
                    // term frequency in current article
                    int termFrequencyInCorpus = combinedFrequency.getTermFrequencyHashMap().get(queryTerm);
                    
                    // length of current article
                    int currentDocumentLength = contentTerms.size();        
                                
                    // summing up the score for each query term of the query
                    score = score + DPHScorer.getDPHScore(occurrences, termFrequencyInCorpus, currentDocumentLength, averageDocumentLengthInCorpus, totalDocsInCorpusValue);

                }
                
            }

            // Calculating average DPH Score for each query to create RankedResult and adding it in a DocumentRanking wrapper
            if(score > 0){

                score = score/query.getQueryTerms().size();

                List<RankedResult> listOfRankedResults = new ArrayList<>();
                RankedResult rankedResult = new RankedResult(value.getId(), value, score);
                listOfRankedResults.add(rankedResult);

                DocumentRanking documentRanking = new DocumentRanking(query, listOfRankedResults);
                listOfDocumentRankings.add(documentRanking);

            }

        }

        return listOfDocumentRankings.iterator();
        
    }

    // Tokenizing first 5 paragraphs of article
    List<String> preProcessContent(NewsArticle value){
        List<String> tokens = new ArrayList<>();
        int paraCounter = 0;
        for(ContentItem item : value.getContents()){

            if (item != null && item.getSubtype() != null && item.getContent() != null && item.getSubtype().equals("paragraph") && paraCounter<5){
                
                paraCounter ++;
                List<String> contentTerms = processor.process(item.getContent());
                tokens.addAll(contentTerms);
            }
            if (paraCounter>=5){
                break;
            }

        }

        return tokens;
    }

}
