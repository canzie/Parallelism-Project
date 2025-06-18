package com.vub.pdproject.search;

import com.vub.pdproject.Util;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;


/**
 *
 * Parallelize the second part of the search, this class implements phase 2 of the project
 * here the text itself will be searched for the keyword occurrences, instead of the article distribution
 *
 * */
public class ParallelSearchOccurrencesTask extends RecursiveTask<Integer>{

    final String keyword;
    final String text;

    // count for how many times the text has been divided, meaning the work for the searching, not the actual text split.
    int curr_splits;
    // a/c = avg characters per article / 100
    // (txt len / (a/c * keyword len)) / (T+1) --> higher T means less splits --> less parallelism
    // if "txt len < keyword len" -> max_splits=0 -> good for small titles
    // on the 'Large' dataset, this means around 300 characters per split when keyword.length = 3
    int max_splits;

    // indices for this task to search from and to.
    int text_start_idx;
    int text_end_idx;

    // average characters per article form the Large dataset / 100
    // To keep things a bit simpler, and to not touch the skeleton code (WikipediaData class),
    // should also help with keeping performance benchmarks between datasets relatively similar (no other variable changes expect #articles and article length)
    static int AVG_C = 47;


    ParallelSearchOccurrencesTask(String keyword, String text, int T) {
        this.keyword = keyword;
        this.text = text;

        // sequential cut-off will be based on max_splits
        // to change the influence of T, a weight could be added to T | ... / (T*w + 1) : 0<w
        //this.max_splits = (text.length()/(AVG_C*keyword.length()))/(T+1);
        this.max_splits = 0;

        this.curr_splits = 0;
        this.text_start_idx = 0;
        this.text_end_idx = text.length()-1;
    }

    private ParallelSearchOccurrencesTask(String keyword, String text, int start, int end, int n_splits, int max_splits) {
        this.keyword = keyword;
        this.text = text;
        this.curr_splits = n_splits;
        this.max_splits = max_splits;

        this.text_start_idx = start;
        this.text_end_idx = end;

    }


    @Override
    protected Integer compute() {

        // no words are left
        if (text_start_idx == text_end_idx){
          return 0;
        // threshold
        } else if (curr_splits == max_splits){
            return countOccurrences();

        }else{
            // every split divides the text in 2 even parts
            int text_middle_word_idx = middleOfTextInterval();

            curr_splits++;
            ParallelSearchOccurrencesTask left_task = new ParallelSearchOccurrencesTask(keyword, text, text_start_idx, text_middle_word_idx, curr_splits, max_splits);
            ParallelSearchOccurrencesTask right_task = new ParallelSearchOccurrencesTask(keyword, text, text_middle_word_idx, text_end_idx, curr_splits, max_splits);
            right_task.fork();
            int left_res = left_task.compute();
            int right_res = right_task.join();

            return left_res + right_res;
        }

    }

    // returns an index pointing to the end of the word located in the middle of the provided interval
    // this index can be used as a 'middle' of the interval for further sub tasking
    // just the middle index could end up with duplicates if the index doesn't point to a whitespace or punctuation mark.
    private int middleOfTextInterval(){

        int middle = (text_end_idx+text_start_idx) / 2;

        for (int i = middle; i<text_end_idx; i++) {
            // return the end(index) of a word
            if (Util.isWhitespaceOrPunctuationMark(text.charAt(i))) {
                return i;
            }
        }
        // if middle starts at the last word, return end --> the right_task can then immediately return 0. start=end
        return text_end_idx;
    }



    // Counts occurrences of a keyword in an interval
    // mostly copied form the provided sequential code,
    // with the addition of being able to search trough intervals of the given text.
    private int countOccurrences() {
        int count = 0;
        int k = 0;


        for (int i = text_start_idx; i < text.length() && i<text_end_idx; i++) {
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
