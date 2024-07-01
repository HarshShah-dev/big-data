package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

// Maps from a tuple to DocumentRanking.
public class TupleToDocRankMap implements MapFunction<Tuple2<Query,DocumentRanking>, DocumentRanking> {

    @Override
    public DocumentRanking call(Tuple2<Query, DocumentRanking> value)  {

        // Returns the DocumentRanking from each tuple
        return value._2;
        
    }
    
}
