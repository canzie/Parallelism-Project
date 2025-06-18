package com.vub.pdproject;

import com.vub.pdproject.benchmark.MeasureRuntimes;
import com.vub.pdproject.data.WikipediaData;
import com.vub.pdproject.data.WikipediaData.WikipediaDataSet;
import com.vub.pdproject.search.QueryEngine;
import com.vub.pdproject.search.QueryEngine.RRecord;
import com.vub.pdproject.search.SequentialSearch;
import com.vub.pdproject.search.ParallelSearch;
import com.vub.pdproject.search.WikipediaQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {


        // Benchmark to be used (`Small`, `Medium`, `Large` on your machine or `Firefly` on Firefly)
        /*WikipediaDataSet benchmark = WikipediaDataSet.Small;

        WikipediaQuery query = WikipediaQuery.forBenchmark(benchmark);

        // Prints out some information about this benchmark
        System.out.println("*** QUERY ***");
        System.out.println(query);

        //QueryEngine qe = new SequentialSearch();
        QueryEngine qe = new ParallelSearch(8, 5);

        assert query != null;
        // Execute query using query engine
        List<RRecord> result_article_ids = query.execute(qe);

        // Output the result of the query (names of articles and their relevance, ordered by decreasing relevance)
        System.out.println();
        System.out.println("*** RESULT ***");
        int i = 1;
        for (RRecord result_article_id : result_article_ids) {
            System.out.println(i + " ) " +
                    query.getData().getArticle(result_article_id.articleID).title +
                    " (" + result_article_id.relevance_score + ")");
            i++;
        }



        //WikipediaDataSet[] wikiDatasetValues = WikipediaDataSet.values();

        //for (WikipediaDataSet wikiDataset : Arrays.copyOfRange(wikiDatasetValues, 0, wikiDatasetValues.length-1)){
        //    System.out.println("Measuring Performance of dataset: " + wikiDataset);

            //MeasureRuntimes MR = new MeasureRuntimes(wikiDataset);
            //MR.Measure(MR.param_configs);
        //}


         */


        // used for benchmarking
        MeasureRuntimes MR = new MeasureRuntimes(WikipediaDataSet.Large, "v3");
        MR.Measure(MR.param_configs);
    }
}