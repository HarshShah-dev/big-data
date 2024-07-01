package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

public class FilterNewsArticles implements FlatMapFunction<NewsArticle, NewsArticle> {

    Broadcast<List<Query>> queryBroadcast;
    LongAccumulator totalDocsInCorpus;
    LongAccumulator sumOfDocLengths;

    private transient TextPreProcessor processor;

    public FilterNewsArticles(Broadcast<List<Query>> queryBroadcast, LongAccumulator totalDocsInCorpus, LongAccumulator sumOfDocLengths) {
        this.queryBroadcast = queryBroadcast;
        this.totalDocsInCorpus = totalDocsInCorpus;
        this.sumOfDocLengths = sumOfDocLengths;
    }
    

    @Override
    public Iterator<NewsArticle> call(NewsArticle t) {
        // Calsulating total number of Documents in Corpus
        totalDocsInCorpus.add(1);

        List <NewsArticle> articleList = new ArrayList<>();
        boolean occurrences = false;

        if (processor==null) processor = new TextPreProcessor();

        // Tokenizing the article
        List<String> contentTerms = new ArrayList<>();
        if(t.getTitle() != null){
            contentTerms.addAll(processor.process(t.getTitle()));
        }
        List<String> contentTerms2 = preProcessContent(t);
        contentTerms.addAll(contentTerms2);

        // Summing up all the document lengths, used for calsulating average doc length
        sumOfDocLengths.add(contentTerms.size());

        // Looping through each query and query term to check if the term occurs in current article.
        for(Query query : queryBroadcast.value()){

            List<String> queryTerms = query.getQueryTerms();

            for(String queryTerm : queryTerms){

                if (Collections.frequency(contentTerms, queryTerm) > 0){
                    occurrences = true;
                    
                }
            }

            
        }

        // Adding the article to a list iff article contains any of the query terms
        if(occurrences == true){
            articleList.add(t);
        }

        return articleList.iterator();        
    
    }

    // Tokenizing the first 5 paragraphs of the Article
    List<String> preProcessContent(NewsArticle t){
        
        List<String> tokens = new ArrayList<>();
        int paraCounter = 0;

        for(ContentItem item : t.getContents()){

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

