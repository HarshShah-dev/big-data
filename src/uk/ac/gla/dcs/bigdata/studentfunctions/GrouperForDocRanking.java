package uk.ac.gla.dcs.bigdata.studentfunctions;


import org.apache.spark.api.java.function.MapFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

// Returns the query for each DocumentRanking using MapFunction
public class GrouperForDocRanking implements MapFunction<DocumentRanking,Query> {

    @Override
    public Query call(DocumentRanking value) {

        return value.getQuery();
        
    }

    
}
