package com.vub.pdproject.benchmark;

import com.vub.pdproject.data.WikipediaData;
import com.vub.pdproject.search.ParallelSearch;
import com.vub.pdproject.search.QueryEngine;
import com.vub.pdproject.search.SequentialSearch;
import com.vub.pdproject.search.WikipediaQuery;

import java.io.*;
import java.util.*;

// class used to run benchmarks and save runtime results
public class MeasureRuntimes {

    // these 2 params config vars are used to
    public Map<Character, Integer[]> param_configs = new HashMap<Character, Integer[]>();
    public Map<Character, Integer[]> param_configs_firefly = new HashMap<Character, Integer[]>();

    public Map<Character, Integer[]> T_optimizer_params = new HashMap<Character, Integer[]>();

    public final WikipediaQuery query;
    public final WikipediaData.WikipediaDataSet datasetSize;

    // amount of repetitions used before measuring performance
    final int WARMUP_REPS = 5;

    final String version;

    // variable used to keep track of how long it took for the dataset to load
    // has no real value to the benchmark, just interesting to know
    final long dataset_load_time_ns;

    public MeasureRuntimes(WikipediaData.WikipediaDataSet datasetSize, String version) throws IOException {
        this.version = version;

        long before = System.nanoTime(); //time is measured in ns

        // load the dataset, should only be done once per dataset per run of the application
        this.query = WikipediaQuery.forBenchmark(datasetSize);

        this.dataset_load_time_ns = System.nanoTime() - before;

        // used for logging after dataset initialization
        this.datasetSize = datasetSize;

        // parameters to test and run benchmarks
        // (these might not be all the values that were tested, some values have been tested on a different run)
        param_configs.put('p', new Integer[] {1, 2, 4, 8});
        param_configs.put('T', new Integer[] {1, 32, 64, 128, 256, 512, 1024, 2048});


        param_configs_firefly.put('p', new Integer[] {1, 2, 4, 8, 16, 32, 48, 64, 96, 128});
        param_configs_firefly.put('T', new Integer[] {1, 32, 64, 128, 256, 512, 1024, 2048});


        // parameters used to find an optimal T
        // handy to test smaller changes in T, before long/large benchmarks
        T_optimizer_params.put('p', new Integer[] {4, 8, 12});
        T_optimizer_params.put('T', new Integer[] {32, 64, 128, 256, 1024});

    }


    // Function to call when running the benchmarks
    // takes in a parameter config and writes to files located in the './runtimes/' directory
    public void Measure(Map<Character, Integer[]>  params) {

        // create directories if not already present
        File theDir = new File("./runtimes/"+version+"/");
        if (!theDir.exists()){
            theDir.mkdirs();
        }
        theDir = new File("./runtimes/"+version+"/"+datasetSize);
        if (!theDir.exists()){
            theDir.mkdirs();
        }


        // benchmarking parallel with a set of different workers and thresholds
        for (Integer p : params.get('p')){
            for (Integer T : params.get('T')) {

                File res_file = new File("./runtimes/"+version+"/"+datasetSize+"/runtimes_T"+T+"_p"+p+".csv");
                if(res_file.exists()){
                    //avoids accidentally overwriting previous results
                    System.out.println(res_file+" already exists");
                    continue;
                }

                QueryEngine qe = new ParallelSearch(p, T);
                // collect the runtimes of this config
                List<Long> rts = benchmark(qe, 5);
                // calculate different metrics
                RuntimeEstimate rte = new RuntimeEstimate(rts);
                // save the results to a file
                write2file(res_file,rte, rts);
            }
        }


        // similar to the loop above, just for the sequential runtimes
        File res_file = new File("./runtimes/"+version+"/"+datasetSize+"/runtimes_seq.csv");
        if(res_file.exists()){
            //avoids accidentally overwriting previous results
            System.out.println(res_file+" already exists");
        } else {
            // benchmarking for sequential results
            QueryEngine sqe = new SequentialSearch();
            List<Long> rts = benchmark(sqe, 5);
            RuntimeEstimate rte = new RuntimeEstimate(rts);
            write2file(res_file,rte, rts);

        }

        // similar to the loop above, just for the runtimes with 1 worker and max parallelism
        // results are used to calculate the overhead.
        res_file = new File("./runtimes/"+version+"/"+datasetSize+"/runtimes_Tinf_p1.csv");
        if(res_file.exists()){
            //avoids accidentally overwriting previous results
            System.out.println(res_file+" already exists");
        } else {
            // benchmarking parallel with P=1 and T=inf
            QueryEngine pqe = new ParallelSearch(1);
            List<Long> rts = benchmark(pqe, 5);
            RuntimeEstimate rte = new RuntimeEstimate(rts);
            write2file(res_file,rte, rts);
        }



    }

    // runs the actual searches and collects the runtimes
    // nrpes = number of actual repetitions, #reps after the warmup repetitions
    public List<Long> benchmark(QueryEngine qe, int nrep){
        List<Long> runtimes = new ArrayList<Long>(nrep);
        System.out.println("\nBenchmark:");
        for(int i = 0; i < nrep+WARMUP_REPS; i++){
            System.out.println(i+1+"/"+(nrep+WARMUP_REPS));

            System.gc(); //a heuristic to avoid Garbage Collection (GC) to take place in timed portion of the code
            long before = System.nanoTime(); //time is measured in ns

            query.execute(qe); //store results to avoid code getting optimised away

            // only keep the runtime measurements, if the compiler has optimized a little.
            if (i>=WARMUP_REPS) {
                runtimes.add(System.nanoTime() - before);
            }

        }
        return runtimes;
    }

    // writes the runtimes to a file
    private void write2file(File f, RuntimeEstimate runtimeMetrics, List<Long> runtimes){
        PrintWriter csv_writer;
        try {
            csv_writer = new PrintWriter(new FileOutputStream(f,true));
            String line = "datasetsize: "+datasetSize+" | loadtime: "+dataset_load_time_ns+" | ";
            for(Long rt : runtimes){
                line += rt+", ";
            }

            line += "\n" + runtimeMetrics.toString();

            csv_writer.println(line);
            csv_writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    // Calculates different metrics based on the runtimes of a configuration
    // mostly taken from the benchmarking WPO
    static class RuntimeEstimate{
        double mean;
        double sd;
        double n;
        double min;
        double median;
        double max;

        RuntimeEstimate(List<Long> runtimes){
            mean = compute_avg(runtimes);
            sd = compute_sd(runtimes);
            n = runtimes.size();
            Collections.sort(runtimes);
            min = runtimes.get(0);
            median = runtimes.get(runtimes.size()/2);
            max = runtimes.get(runtimes.size()-1);
        }

        double getStandardError(){
            return sd/Math.sqrt(n);
        }

        public String toString(){
            return "mean: "+pp(mean)+" +- "+pp(1.96*getStandardError())+" [min:"+pp(min)+", median:"+pp(median)+", max:"+pp(max)+"] sec";
        }

        private double pp(double rt){
            return (double) rt/1000000000;
        }

        static private double compute_avg(List<Long> runtimes){
            double sum = 0;
            for(Long rt : runtimes){
                sum += rt;
            }
            return sum/runtimes.size();
        }

        static private double compute_sd(List<Long> runtimes){
            double mean = compute_avg(runtimes);
            double sum = 0;
            for(Long rt : runtimes){
                sum += (rt-mean)*(rt-mean);
            }
            return Math.sqrt(sum/runtimes.size());
        }

    }

}
