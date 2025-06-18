package com.vub.pdproject.search;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.vub.pdproject.Util;
import com.vub.pdproject.data.WikipediaData;
import com.vub.pdproject.data.models.Article;

/**
 * Sequential implementation of QueryEngine
 *
 * @author Aäron Munsters
 */
public class SequentialSearch implements QueryEngine {

    @Override
    public List<RRecord> search(String keyword, WikipediaData data) {
        // loop over articles, determine their relevance & add them to a list if relevance > 0
        List<RRecord> relevant_articles = new ArrayList<>();
        for (String article_id : data.getArticleIDs()) {
            double relevance = evaluate_relevance(keyword, article_id, data);
            if (relevance > 0) {
                relevant_articles.add(new RRecord(article_id, relevance));
            }
        }
        // sort the resulting articles by decreasing relevance
        Collections.sort(relevant_articles);
        return relevant_articles;
    }

    /**
     * @param keyword   A single keyword (without spaces)
     * @param articleID The id of the article whose relevance will be determined
     * @param data      The data to be searched
     * @return The relevance of given article for a given keyword
     */
    public static double evaluate_relevance(String keyword, String articleID, WikipediaData data) {
        // fetch data for article
        Article article = data.getArticle(articleID);

        // check how many times query string appears in text
        int occurrences = 0;
        occurrences += countOccurrences(keyword, article.text);

        // calculate relevance score
        double relevance_score = 0;
        if (countOccurrences(keyword, article.title) > 0)
            relevance_score = 0.5;

        relevance_score += 1.5 * occurrences / (occurrences + 20);
        return relevance_score;
    }

    /**
     * @param keyword The keyword to be searched for
     * @param text    The text to be searched.
     * @return The number of occurrences of a keyword in a text
     * Search is case-sensitive.
     * To match, a word in text must be delimited by white space/punctuation marks
     * (or appear at the beginning/end of the text)
     * <p>
     * examples:
     * <p>
     * keyword: burger
     * <p>
     * text: "This burger was so good!" (returns 1)
     * text: "Great burger" (returns 1)
     * text: "Great burger!" (returns 1)
     * text: "Great Burger" (returns 0)
     * text: "burgers don't get any better!" (returns 0)
     */
    public static int countOccurrences(String keyword, String text) {
        int count = 0;
        int k = 0;
        for (int i = 0; i < text.length(); i++) {
            if (Util.isWhitespaceOrPunctuationMark(text.charAt(i))) {
                if (k == keyword.length()) {
                    count++;
                }
                k = 0;
            } else if (k >= 0) {
                if (k < keyword.length() && text.charAt(i) == keyword.charAt(k)) {
                    k++;
                } else {
                    k = -1;
                }
            }
        }

        if (k == keyword.length()) {
            count++;
        }
        return count;
    }

}
