package com.caseystella.analytics.streaming.outlier.mad;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.Distribution;
import com.caseystella.analytics.streaming.outlier.OutlierAlgorithm;
import com.caseystella.analytics.streaming.outlier.Severity;
import com.caseystella.analytics.util.JSONUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SketchyMovingMAD implements OutlierAlgorithm{
    private SketchyMovingMADConfig config;
    private Map<String, Distribution.Context> valueDistributions = new HashMap<>();
    private Map<String, Distribution.Context> medianDistributions = new HashMap<>();

    public SketchyMovingMAD() {

    }
    public SketchyMovingMAD(SketchyMovingMADConfig config) {
        this.config = config;
    }

    public Map<String, Distribution.Context> getMedianDistributions() {
        return medianDistributions;
    }

    public SketchyMovingMADConfig getConfig() {
        return config;
    }

    public Map<String, Distribution.Context> getValueDistributions() {
        return valueDistributions;
    }

    public static final double ZSCORE = 0.6745;
    @Override
    public Severity analyze(DataPoint dp) {
        Distribution.Context valueDistribution = getContext(dp.getSource(), valueDistributions);
        Distribution.Context medianDistribution = getContext(dp.getSource(), medianDistributions);
        boolean haveEnoughValues = valueDistribution.getAmount() > config.getMinAmountToPredict();
        boolean haveEnoughMedians =medianDistribution.getAmount() > config.getMinAmountToPredict();
        boolean makePrediction = haveEnoughValues
                              && haveEnoughMedians
                              ;
        Severity ret = Severity.NOT_ENOUGH_DATA;
        Double absDiff = null;
        Double median = null;
        if(haveEnoughValues) {
            median = valueDistribution.getCurrentDistribution().getMedian();
        }
        valueDistribution.addDataPoint(dp, config.getRotationPolicy(), config.getChunkingPolicy());
        if(makePrediction) {
            double mad = medianDistribution.getCurrentDistribution().getMedian();
            double zScore = ZSCORE*(dp.getValue() - median)/mad;
            ret = getSeverity(zScore);
        }
        if(haveEnoughValues) {
            absDiff = Math.abs(dp.getValue() - median);
            medianDistribution.addDataPoint(new DataPoint(dp.getTimestamp(), absDiff, dp.getMetadata(), dp.getSource())
                                            , config.getRotationPolicy(), config.getChunkingPolicy()
                                           );
        }
        return ret;
    }

    private Severity getSeverity(double zScore) {
        if(zScore < config.getzScoreCutoffs().get(Severity.NORMAL)) {
            return Severity.NORMAL;
        }
        else if(zScore < config.getzScoreCutoffs().get(Severity.MODERATE_OUTLIER)) {
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

    @Override
    public void configure(String configStr) {
        try {
            config = JSONUtil.INSTANCE.load(configStr, SketchyMovingMADConfig.class);
            boolean cutoffsAreThere = config.getzScoreCutoffs().get(Severity.MODERATE_OUTLIER) != null
                                   && config.getzScoreCutoffs().get(Severity.NORMAL) != null
                    ;
            if(!cutoffsAreThere) {
                throw new IllegalStateException("You must specify NORMAL, SEVERE_OUTLIER and MODERATE_OUTLIER cutoffs in the severity map");
            }
            boolean proper = config.getzScoreCutoffs().get(Severity.NORMAL) <= config.getzScoreCutoffs().get(Severity.MODERATE_OUTLIER);
            if(!proper) {
                throw new IllegalStateException("You must specify a severity map which is well ordered (NORMAL <= MODERATE_OUTLIER <= SEVERE_OUTLIER");
            }
        } catch (IOException e) {
            throw new IllegalStateException("Cannot configure SketchyMovingMAD outlier algorithm.", e);
        }
    }
}
