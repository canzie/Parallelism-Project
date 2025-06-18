package com.vub.pdproject.search;

import java.io.IOException;
import java.util.List;

import com.vub.pdproject.data.WikipediaData;
import com.vub.pdproject.search.QueryEngine.RRecord;

/**
 * Convenience class to represent a whole query:
 * Combining both keyword and data to be searched
 *
 * @author Aäron Munsters
 */
public class WikipediaQuery {
    private final String keyword;
    private final WikipediaData data;

    private WikipediaQuery(String keyword, WikipediaData data) {
        this.keyword = keyword;
        this.data = data;
    }

    /**
     * @return The keyword to search for
     */
    public String getKeyword() {
        return keyword;
    }

    /**
     * @return The data to be searched
     */
    public WikipediaData getData() {
        return data;
    }

    /**
     * Executes this query, using a given query engine.
     *
     * @param queryEngine: the query engine to be used to resolve this query
     * @return the query result (business ids, their relevance, ordered by decreasing relevance)
     */
    public List<RRecord> execute(QueryEngine queryEngine) {
        return queryEngine.search(keyword, data);
    }

    public String toString() {
        return "keyword: " + keyword + System.lineSeparator() + "data: " + System.lineSeparator() + data;
    }

    /**
     * Constructs the queries to be used to benchmark your application.
     *
     * @param wikipediaDataSet: the identifier of benchmark, also corresponds to the wikipediaDataSet of the data preset used.
     * @return benchmark with given index
     */
    public static WikipediaQuery forBenchmark(WikipediaData.WikipediaDataSet wikipediaDataSet) throws IOException {
        switch (wikipediaDataSet) {
            case Small:
                return new WikipediaQuery("between", WikipediaData.forPreset(wikipediaDataSet));
            case Medium:
                return new WikipediaQuery("text", WikipediaData.forPreset(wikipediaDataSet));
            case Large:
                return new WikipediaQuery("the", WikipediaData.forPreset(wikipediaDataSet));
            case Firefly:
                return new WikipediaQuery("the", WikipediaData.forFirefly());
        }
        return null;
    }
}

