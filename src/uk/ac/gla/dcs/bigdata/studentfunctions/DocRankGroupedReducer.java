package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

// Reduces multiple document rankings per query to one for each query
// Also filters the RankedResults based on title similarity and returns the top 10 RankedResults for each DocumentRanking
// based on dph score
public class DocRankGroupedReducer implements ReduceFunction<DocumentRanking> {

    @Override
    public DocumentRanking call(DocumentRanking v1, DocumentRanking v2) throws Exception {

        List<RankedResult> newList = new ArrayList<>();

        // Reducing the List of RankedResults
        newList = Stream.concat(v1.getResults().stream(), v2.getResults().stream()).collect(Collectors.toList());
        
        // Sorting in descending order
        Collections.sort(newList, Collections.reverseOrder());

        // List of similar titles
        List<RankedResult> blackList = new ArrayList<>();

        // Adding articles with similar titles to blackList
        for (int i = 0; i < newList.size(); i++){
            if (blackList.contains(newList.get(i))){
                continue;
            }
            for (int j = i+1; j < newList.size(); j++){
                String title1 = newList.get(i).getArticle().getTitle();
                String title2 = newList.get(j).getArticle().getTitle();
                if (title1 != null && title2 != null){
                    if(TextDistanceCalculator.similarity(title1, title2) < 0.5 ){
                        blackList.add(newList.get(j));
                    }
                }
            }
            }

            // Removing articles with simialar titles from the list
            for(int i = 0; i < newList.size(); i++){
                if (blackList.contains(newList.get(i))){
                    newList.remove(i);
                    i--;
                }

            }
        
        // Taking the top 10 articles from list
        List<RankedResult> top10Docs = newList.subList(0, Math.min(newList.size(), 10));

    
        // Returning a DocumentRanking for each query.
        DocumentRanking newDoc = new DocumentRanking(v1.getQuery(), top10Docs);
        return newDoc;
        
    }

}
