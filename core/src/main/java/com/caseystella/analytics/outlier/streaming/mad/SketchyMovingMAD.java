package com.caseystella.analytics.outlier.streaming.mad;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.distribution.Distribution;
import com.caseystella.analytics.distribution.scaling.ScalingFunctions;
import com.caseystella.analytics.distribution.SimpleTimeRange;
import com.caseystella.analytics.outlier.streaming.OutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.OutlierConfig;
import com.caseystella.analytics.outlier.Severity;

import java.util.*;

public class SketchyMovingMAD implements OutlierAlgorithm{
    public static final double ZSCORE = 0.6745;
    private static final double EPSILON = 1e-4;
    private OutlierConfig config;
    private Map<String, Distribution.Context> valueDistributions = new HashMap<>();
    private Map<String, Distribution.Context> medianDistributions = new HashMap<>();
    private Map<String, Distribution.Context> zScoreDistributions = new HashMap<>();
    private Map<Severity, Double> zScoreCutoffs = new EnumMap<>(Severity.class);
    private long minAmountToPredict = 100;
    private double minPercentileZScoreToAllow = 0.95;
    private LinkedList<Severity> lastOutlier = new LinkedList<>();
    public SketchyMovingMAD() {

    }
    public SketchyMovingMAD(OutlierConfig config) {
        configure(config);
    }

    public SketchyMovingMAD withConfig(OutlierConfig config) {
        configure(config);
        return this;
    }

    public Map<String, Distribution.Context> getMedianDistributions() {
        return medianDistributions;
    }

    public OutlierConfig getConfig() {
        return config;
    }

    public Map<String, Distribution.Context> getValueDistributions() {
        return valueDistributions;
    }

    @Override
    public Outlier analyze(DataPoint dp) {
        if(config == null) {
            throw new RuntimeException("Outlier Algorithm is not configured yet.");
        }
        Distribution.Context valueDistribution = getContext(dp.getSource(), valueDistributions);
        Distribution.Context medianDistribution = getContext(dp.getSource(), medianDistributions);
        Distribution.Context zScoreDistribution = getContext(dp.getSource(), zScoreDistributions);
        boolean haveEnoughValues = valueDistribution.getAmount() > minAmountToPredict && dp.getValue() > EPSILON;
        boolean haveEnoughMedians =medianDistribution.getAmount() > minAmountToPredict;
        boolean makePrediction = haveEnoughValues
                              && haveEnoughMedians
                              ;
        Severity ret = Severity.NOT_ENOUGH_DATA;
        Double absDiff = null;
        Double median = null;
        Double mean = null;
        if(haveEnoughValues) {
            mean = scaleValue(valueDistribution.getCurrentDistribution().getMean());
            median = valueDistribution.getCurrentDistribution().getMedian();
        }
        valueDistribution.addDataPoint(dp, config.getRotationPolicy(), config.getChunkingPolicy(), config.getScalingFunction(), config.getGlobalStatistics());
        Double zScore = null;
        if(makePrediction) {
            double k = ZSCORE;
            double mad = medianDistribution.getCurrentDistribution().getMedian();
            if(mad < EPSILON) {
                ret = Severity.NORMAL;
            }
            else {
                double medianScaledPt = scalePoint(dp) - median;
                zScore = Math.abs(k * medianScaledPt / mad);
                ret = getSeverity(zScore);
                if(zScoreDistribution.getAmount() > minAmountToPredict) {
                    if(ret == Severity.SEVERE_OUTLIER) {
                        double topPercentile = zScoreDistribution.getCurrentDistribution().getPercentile(minPercentileZScoreToAllow);
                        if(zScore < topPercentile) {
                            ret = Severity.MODERATE_OUTLIER;
                        }
                    }
                }
                if(zScore > EPSILON) {
                    zScoreDistribution.addDataPoint(new DataPoint(0, zScore, null, dp.getSource())
                            , config.getRotationPolicy(), config.getChunkingPolicy()
                            , ScalingFunctions.NONE //don't want to scale the values in the MAD distribution.
                            , null //we don't know the global statistics here.
                    );
                }
                else {
                    if(Math.abs(dp.getValue()) < EPSILON) {
                        try {
                            Thread.sleep(0);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        if(haveEnoughValues) {
            absDiff = Math.abs(scalePoint(dp) - median);
            if(absDiff > EPSILON) {
                medianDistribution.addDataPoint(new DataPoint(dp.getTimestamp(), absDiff, dp.getMetadata(), dp.getSource())
                        , config.getRotationPolicy(), config.getChunkingPolicy()
                        , ScalingFunctions.NONE //don't want to scale the values in the MAD distribution.
                        , null //we don't know the global statistics here.
                );
            }
        }
        Outlier o = new Outlier(dp, ret, new SimpleTimeRange(valueDistribution.getCurrentDistribution()), zScore);
        adjustSeverity(o);
        return o;
    }

    private void adjustSeverity(Outlier s) {
        if(s.getSeverity() == Severity.SEVERE_OUTLIER
        && lastOutlier != null
        && (lastOutlier.contains(Severity.SEVERE_OUTLIER))
          )
        {
            //s.setSeverity(Severity.NORMAL);
        }
        lastOutlier.addFirst(s.getSeverity());
        if(lastOutlier.size() > 3) {
            lastOutlier.removeLast();
        }
    }

    private double scaleValue(double val) {
        return config.getScalingFunction().scale(val, config.getGlobalStatistics());
    }

    private double scalePoint(DataPoint dp) {
        return scaleValue(dp.getValue());
    }

    private Severity getSeverity(double zScore) {
        if(zScore < zScoreCutoffs.get(Severity.NORMAL)) {
            return Severity.NORMAL;
        }
        else if(zScore < zScoreCutoffs.get(Severity.MODERATE_OUTLIER)) {
            return Severity.MODERATE_OUTLIER;
        }
        return Severity.SEVERE_OUTLIER;
    }

    private Distribution.Context getContext(String source, Map<String, Distribution.Context> contextMap) {
        Distribution.Context context = contextMap.get(source);
        if(context == null) {
            context = new Distribution.Context();
            contextMap.put(source, context);
        }
        return context;
    }
    public static final String ZSCORE_CUTOFFS_CONF = "zscoreCutoffs";
    public static final String MIN_AMOUNT_TO_PREDICT = "minAmountToPredict";
    public static final String MIN_ZSCORE_PERCENTILE = "minZscorePercentile";


    public static Long coerceLong(String field, Object o) {
        if(o instanceof String) {
            return Long.parseLong(o.toString());
        }
        else if(o instanceof Number) {
            return ((Number)o).longValue();
        }
        throw new RuntimeException(field + ": Unable to coerce " + o + " to a number");
    }

    public static Double coerceDouble(String field, Object o) {
        if(o instanceof String) {
            return Double.parseDouble(o.toString());
        }
        else if(o instanceof Number) {
            return ((Number)o).doubleValue();
        }
        throw new RuntimeException(field + ": Unable to coerce " + o + " to a number");
    }

    @Override
    public void configure(OutlierConfig config) {
        this.config = config;
        if(config.getConfig().containsKey(MIN_AMOUNT_TO_PREDICT)) {
            Object o = config.getConfig().get(MIN_AMOUNT_TO_PREDICT);
            minAmountToPredict = coerceLong(MIN_AMOUNT_TO_PREDICT, o);
        }

        if(config.getConfig().containsKey(ZSCORE_CUTOFFS_CONF)) {
            Map<Object, Object> map = (Map<Object, Object>) config.getConfig().get(ZSCORE_CUTOFFS_CONF);
            for(Map.Entry<Object, Object> kv : map.entrySet()) {
                zScoreCutoffs.put(Severity.valueOf(kv.getKey().toString()), coerceDouble(MIN_AMOUNT_TO_PREDICT, kv.getValue()));
            }
        }
        boolean cutoffsAreThere = zScoreCutoffs.get(Severity.MODERATE_OUTLIER) != null
                && zScoreCutoffs.get(Severity.NORMAL) != null
                ;
        if(!cutoffsAreThere) {
            throw new IllegalStateException("You must specify NORMAL, SEVERE_OUTLIER and MODERATE_OUTLIER cutoffs in the severity map");
        }
        boolean proper = zScoreCutoffs.get(Severity.NORMAL) <= zScoreCutoffs.get(Severity.MODERATE_OUTLIER);
        if(!proper) {
            throw new IllegalStateException("You must specify a severity map which is well ordered (NORMAL <= MODERATE_OUTLIER <= SEVERE_OUTLIER");
        }
        if(config.getConfig().containsKey(MIN_ZSCORE_PERCENTILE)) {
            Object o = config.getConfig().get(MIN_ZSCORE_PERCENTILE);
            minPercentileZScoreToAllow = coerceDouble(MIN_ZSCORE_PERCENTILE, o)/100;

        }
    }
}
