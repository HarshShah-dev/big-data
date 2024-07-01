package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;


// Calculates the query term frequency in each article, required to calculate the DPH scores.
public class CalculateNewsMetadata implements FlatMapFunction<NewsArticle, TermFrequency>{
    Broadcast<List<Query>> queryBroadcast;
    private transient TextPreProcessor processor;

    public CalculateNewsMetadata(Broadcast<List<Query>> queryBroadcast) {
                this.queryBroadcast = queryBroadcast;
    }

    @Override
    public Iterator<TermFrequency> call(NewsArticle value)  {

        List <TermFrequency> frequencyList = new ArrayList<>();
        if (processor==null) processor = new TextPreProcessor();

        Map<String, Integer> freqMap = new HashMap<>();
        List<String> contentTerms = new ArrayList<>();

        // Tokenizing the article using TextPreProcessor
        if(value.getTitle() != null){
            contentTerms.addAll(processor.process(value.getTitle()));
        }
        List<String> contentTerms2 = preProcessContent(value);
        contentTerms.addAll(contentTerms2);

        // Looping through each query to calculate the occurences for each term in article
        for(Query query : queryBroadcast.value()){
            
            List<String> queryTerms = query.getQueryTerms();
            
            for(String queryTerm : queryTerms){
                // Calculates occurence of term in article
                int occurrences = Collections.frequency(contentTerms, queryTerm);

                // Adds the term - occurence pair to a HashMap
                if(occurrences>0){
                    freqMap.put(queryTerm, freqMap.getOrDefault(queryTerm, 0)+occurrences);
                }
                
            }     
        }

        // Adding TermFrequency to list if Hashmap is not empty
        if(!freqMap.isEmpty()){

            frequencyList.add(new TermFrequency(freqMap));
        }

        // Returns a list of Hashmaps(TermFreuency) containing freq
        return frequencyList.iterator();        
    
    }

    // Tokenizing first 5 paragraphs of the article
    List<String> preProcessContent(NewsArticle value){
        List<String> tokens = new ArrayList<>();
        int paraCounter = 0;
        for(ContentItem item : value.getContents()){

            if (item != null && item.getContent() != null && item.getSubtype() != null && item.getSubtype().equals("paragraph") && paraCounter<5){
                
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
