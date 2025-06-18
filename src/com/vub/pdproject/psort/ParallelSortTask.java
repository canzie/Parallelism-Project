package com.vub.pdproject.psort;

import com.vub.pdproject.pack.BitvectorPartition;
import com.vub.pdproject.pack.Pack;
import com.vub.pdproject.pack.PrefixSum;
import com.vub.pdproject.search.QueryEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import static java.lang.Integer.max;
import static java.lang.Integer.min;


public class ParallelSortTask extends RecursiveTask<ArrayList<QueryEngine.RRecord>> {
    private int low;
    private int high;
    private ArrayList<QueryEngine.RRecord> relevant_articles;
    private int T;


    public ParallelSortTask(ArrayList<QueryEngine.RRecord> relevant_articles, int T) {
        this.T = T;
        this.low = 0;
        this.high = relevant_articles.size()-1;

        this.relevant_articles = relevant_articles;
    }

    // get the median between the first middle and last element to determine the pivot

    private int getPivotIndex(){
        QueryEngine.RRecord first = relevant_articles.get(0);
        int mid_index = relevant_articles.size()/2;
        QueryEngine.RRecord mid = relevant_articles.get(mid_index);
        QueryEngine.RRecord last = relevant_articles.get(relevant_articles.size() - 1);

        if ((first.compareTo(mid) < 0 && mid.compareTo(last) < 0) || ((last.compareTo(mid) < 0 && mid.compareTo(first) < 0))){
            return mid_index;
        } else if ((mid.compareTo(first) < 0 && first.compareTo(last) < 0) || ((last.compareTo(first) < 0 && first.compareTo(mid) < 0))){
            return 0;
        } else {
            return relevant_articles.size() - 1;
        }
    }
    private QueryEngine.RRecord getPivot(){
        return relevant_articles.get(getPivotIndex());
    }


    @Override
    protected ArrayList<QueryEngine.RRecord> compute() {
        if (high - low < T) {
            Collections.sort(relevant_articles);
            return relevant_articles;
        } else {
            QueryEngine.RRecord pivot = getPivot();
            relevant_articles.remove(getPivotIndex());
            this.high--;

            //ArrayList<QueryEngine.RRecord> lesser = packParallel(low, max(getPivotIndex()-1, 0), true);
            //ArrayList<QueryEngine.RRecord> greater = packParallel(min(getPivotIndex()+1, high), high, false);

            ArrayList<QueryEngine.RRecord> lesser = packParallel(pivot, true);
            ArrayList<QueryEngine.RRecord> greater = packParallel(pivot, false);

            ParallelSortTask left_task = new ParallelSortTask(lesser, T);
            ParallelSortTask right_task = new ParallelSortTask(greater, T);
            right_task.fork();

            ArrayList<QueryEngine.RRecord> left_res =  left_task.compute();
            ArrayList<QueryEngine.RRecord> right_res = right_task.join();

            // reserve space for small performance improvement
            ArrayList<QueryEngine.RRecord> output = new ArrayList<QueryEngine.RRecord>(left_res.size()+right_res.size()+1);
            output.addAll(left_res);
            output.add(pivot);
            output.addAll(right_res);

            return output;
        }

    }

    private ArrayList<QueryEngine.RRecord> packParallel(QueryEngine.RRecord pivot, boolean calc_lesser){
        if (high==low)
            return new ArrayList<QueryEngine.RRecord>();

        int[] bitVectorOutput = new int[relevant_articles.size()];
        BitvectorPartition bitVector = new BitvectorPartition(relevant_articles, bitVectorOutput, low, high+1, calc_lesser, pivot);
        bitVector.compute();


        // step 2: perform parallel prefix sum on the bit vector produced in the previous step.
        final int[] bitSum = PrefixSum.prefixSumParallel(bitVectorOutput);

        // step 3: perform parallel map to assign input elements to bitsum[i]-1 in the
        // output array if bitvector[i] is set
        int out_length = bitSum[bitSum.length-1];
        ArrayList<QueryEngine.RRecord> output = new ArrayList<>(out_length);
        new Pack(relevant_articles, output, low, high+1, bitVectorOutput, bitSum).compute();

        return output;

    }


}
