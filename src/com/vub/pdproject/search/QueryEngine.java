package com.vub.pdproject.search;

import com.vub.pdproject.data.WikipediaData;

import java.util.List;

public interface QueryEngine {
    /**
     * Searches a given dataset, for articles relevant for a given keyword.
     *
     * @param keyword: A single word (not containing any white space characters)
     * @param data:    The subset of articles to be searched.
     * @return A list of articles and their non-zero relevance, sorted by decreasing relevance.
     */
    List<RRecord> search(String keyword, WikipediaData data);

    /*
     * Record storing an article ID alongside its relevance.
     * Its natural order is by decreasing relevance.
     */
    class RRecord implements Comparable<RRecord> {
        public String articleID;
        public double relevance_score;

        RRecord(String articleID, double relevance_score) {
            this.articleID = articleID;
            this.relevance_score = relevance_score;
        }

        @Override
        public int compareTo(RRecord other) {
            return relevance_score != other.relevance_score
                    ? Double.compare(other.relevance_score, relevance_score)
                    : articleID.compareTo(other.articleID);
        }
    }
}
