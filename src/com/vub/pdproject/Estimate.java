package com.vub.pdproject;

import java.util.ArrayList;
import java.util.List;

/**
 * A utility class for measuring mean/standard error of run times and ratios of run times.
 *
 * @author Aäron Munsters
 */
@SuppressWarnings("unused") // TODO: remove this line if you put this class to use
public class Estimate {

    /**
     * Calculates an estimate of the mean runtime, based on runtime observations.
     *
     * @param runtimes: A list of runtime measurements
     * @return The average of these measurements
     */
    static public double meanRuntime(List<Long> runtimes) {
        return mean(listLong2Double(runtimes));
    }

    /**
     * Estimates the standard error on the estimate of the mean runtime, based on runtime observations.
     *
     * @param runtimes: A list of runtime measurements
     * @return The estimate of the standard error
     */
    static public double errorRuntime(List<Long> runtimes) {
        double var = variance(listLong2Double(runtimes));
        return Math.sqrt(var / runtimes.size());
    }

    /**
     * Calculates an estimate of the mean of T_A/T_B.
     *
     * @param rts_nominator:   Observations of run times T_A
     * @param rts_denominator: Observations of run times T_B
     * @return The average of the runtime ratios obtained by pairing observations for T_A and T_B.
     */
    static public double meanRuntimeRatio(List<Long> rts_nominator, List<Long> rts_denominator) {
        return mean(pairedRatios(rts_nominator, rts_denominator));
    }

    /**
     * Estimates the standard error on the estimate of the mean of T_A/T_B.
     *
     * @param rts_nominator:   Observations of run times T_A
     * @param rts_denominator: Observations of run times T_B
     * @return The estimate of the standard error
     */
    static public double errorRuntimeRatio(List<Long> rts_nominator, List<Long> rts_denominator) {
        List<Double> ratios = pairedRatios(rts_nominator, rts_denominator);
        double var = variance(ratios);
        return Math.sqrt(var / ratios.size());
    }

    static private List<Double> listLong2Double(List<Long> longs) {
        List<Double> doubles = new ArrayList<>(longs.size());
        for (Long l : longs) {
            doubles.add(l.doubleValue());
        }
        return doubles;
    }

    static private List<Double> pairedRatios(List<Long> rts_nominator, List<Long> rts_denominator) {
        int length = Math.min(rts_nominator.size(), rts_denominator.size());
        List<Double> ratios = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            ratios.add((double) rts_nominator.get(i) / rts_denominator.get(i));
        }
        return ratios;
    }

    static private double mean(List<Double> values) {
        double sum = 0;
        for (Double v : values) {
            sum += v;
        }
        return sum / values.size();
    }

    static private double variance(List<Double> values) {
        double mean = mean(values);
        double squared_deviation_sum = 0;
        for (Double v : values) {
            double deviation = v - mean;
            squared_deviation_sum += deviation * deviation;
        }
        return squared_deviation_sum / values.size();
    }
}