package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.studentstructures.TermFrequency;

// Reduces the TermFerquency HashMap by adding all the frequencies in each HashMap to, eventually, a single Hashmap.
public class FrequencyReducer implements ReduceFunction<TermFrequency>{

    @Override
    public TermFrequency call(TermFrequency v1, TermFrequency v2) throws Exception {
        v1.getTermFrequencyHashMap().forEach((key, value) -> 
            v2.getTermFrequencyHashMap().merge(key, value, (k1, k2) -> k1 + k2)
        );
        return v2;
    }
}
