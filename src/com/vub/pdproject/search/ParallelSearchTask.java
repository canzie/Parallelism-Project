package com.vub.pdproject.search;

import com.vub.pdproject.Util;
import com.vub.pdproject.data.WikipediaData;
import com.vub.pdproject.data.models.Article;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;


/**
 *
 * Parallelize the first part of the search, this class implements phase 1 of the project
 * No text search will be done here, only the distribution of the articles
 *
* */
public class ParallelSearchTask extends RecursiveTask<List<QueryEngine.RRecord>>{

    final String keyword;
    final WikipediaData data;
    final int T;

    // indices represent which article ids the specific task needs to process
    // articles returned form the 'WikipediaData.getArticleIDs()' procedure
    final int start_idx;
    final int end_idx;

    private List<QueryEngine.RRecord> relevant_articles;

    private ParallelSearchTask(String keyword, WikipediaData data, int start_idx, int end_idx, int T) {
        this.keyword = keyword;
        this.data = data;
        this.T = T;
        this.relevant_articles = new ArrayList<>();
        this.start_idx = start_idx;
        this.end_idx = end_idx;
    }

    ParallelSearchTask(String keyword, WikipediaData data, int T) {
        this.keyword = keyword;
        this.data = data;
        this.T = T;
        this.relevant_articles = new ArrayList<>();
        this.start_idx = 0;
        this.end_idx = data.getArticleIDs().size()-1;
    }


    @Override
    protected List<QueryEngine.RRecord> compute() {
        relevant_articles = new ArrayList<>();

        if (end_idx-start_idx == 0) { // last element
            double relevance = evaluate_relevance(keyword, getArticleID(start_idx));
            if (relevance > 0) {
                relevant_articles.add(new QueryEngine.RRecord(getArticleID(start_idx), relevance));
            }


        } else if (end_idx-start_idx <= T) { // Phase 1 cut-off Threshold T
            for (String article_id : data.getArticleIDs().subList(start_idx, end_idx+1)) {
                double relevance = evaluate_relevance(keyword, article_id);
                if (relevance > 0) {
                    relevant_articles.add(new QueryEngine.RRecord(article_id, relevance));
                }
            }

        } else if (end_idx-start_idx > 0) { // The articles can be further divided and the threshold has not been reached
            int middle = (end_idx+start_idx+1)/2;


            ParallelSearchTask left_task = new ParallelSearchTask(keyword, data, start_idx, middle-1, T);
            ParallelSearchTask right_task = new ParallelSearchTask(keyword, data, middle, end_idx, T);
            right_task.fork();
            List<QueryEngine.RRecord> left_res = left_task.compute();
            List<QueryEngine.RRecord> right_res = right_task.join();

            relevant_articles.addAll(left_res);
            relevant_articles.addAll(right_res);

        }

        return relevant_articles;
    }


    private String getArticleID(int index) {
        return data.getArticleIDs().get(index);
    }

    private double evaluate_relevance(String keyword, String articleID) {
        // fetch data for article
        Article article = data.getArticle(articleID);

        // check how many times query string appears in text
        int occurrences = 0;
        occurrences += new ParallelSearchOccurrencesTask(keyword, article.text, T).compute();

        // calculate relevance score
        double relevance_score = 0;
        if (new ParallelSearchOccurrencesTask(keyword, article.title, T).compute() > 0)
            relevance_score = 0.5;

        relevance_score += 1.5 * occurrences / (occurrences + 20);
        return relevance_score;
    }




}

