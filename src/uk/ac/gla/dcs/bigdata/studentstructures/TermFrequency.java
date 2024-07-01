package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

// This class is used as a wrapper to store the Hashmap containing the termFrequencyInCorpus for each query term.
public class TermFrequency implements Serializable{

    Map<String,Integer> termFrequencyHashMap = new HashMap<>();


    @Override
    public String toString() {
        return "TermFrequency [termFrequencyHashMap=" + termFrequencyHashMap + "]";
    }

    public TermFrequency() {
        super();
    }

    public TermFrequency(Map<String, Integer> termFrequencyHashMap) {
        super();
        this.termFrequencyHashMap = termFrequencyHashMap;
    }

    public Map<String, Integer> getTermFrequencyHashMap() {
        return termFrequencyHashMap;
    }

    public void setTermFrequencyHashMap(Map<String, Integer> termFrequencyHashMap) {
        this.termFrequencyHashMap = termFrequencyHashMap;
    }
}
