package com.caseystella.analytics.streaming.outlier;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.streaming.outlier.mad.SketchyMovingMAD;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SketchyMovingMADTest {

    /**
     {
     "rotationPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 100
                       ,"unit" : "POINTS"
                        }
     ,"chunkingPolicy" : {
                        "type" : "BY_AMOUNT"
                       ,"amount" : 10
                       ,"unit" : "POINTS"
                         }
     ,"minAmountToPredict" : 100
     ,"zScoreCutoffs" : {
                        "NORMAL" : 3.5
                       ,"MODERATE_OUTLIER" : 5
                        }
     }
     */
    @Multiline
    public static String madConfig;

    public static double getValAtModifiedZScore(double zScore, double mad, double median) {
        return (mad*zScore)/SketchyMovingMAD.ZSCORE + median;

    }

    @Test
    public void testSketchyMovingMAD() {
        Random r = new Random(0);
        List<DataPoint> points = new ArrayList<>();
        DescriptiveStatistics stats = new DescriptiveStatistics();
        DescriptiveStatistics medianStats = new DescriptiveStatistics();
        SketchyMovingMAD madAlgo = new SketchyMovingMAD();
        madAlgo.configure(madConfig);
        int i = 0;
        for(i = 0; i < 10000;++i) {
            double val = r.nextDouble() * 1000;
            stats.addValue(val);
            DataPoint dp = (new DataPoint(i, val, null, "foo"));
            madAlgo.analyze(dp);
            points.add(dp);
        }
        for(DataPoint dp : points) {
            medianStats.addValue(Math.abs(dp.getValue() - stats.getPercentile(50)));
        }
        double mad = medianStats.getPercentile(50);
        double median = stats.getPercentile(50);
        {
            double val = getValAtModifiedZScore(3.6, mad, median);
            System.out.println("MODERATE => " + val);
            DataPoint dp = (new DataPoint(i++, val, null, "foo"));
            Severity s = madAlgo.analyze(dp);
            Assert.assertEquals(Severity.MODERATE_OUTLIER, s);
        }
        {
            double val = getValAtModifiedZScore(6, mad, median);
            System.out.println("SEVERE => " + val);
            DataPoint dp = (new DataPoint(i++, val, null, "foo"));
            Severity s = madAlgo.analyze(dp);
            Assert.assertEquals(Severity.SEVERE_OUTLIER, s);
        }

        Assert.assertTrue(madAlgo.getMedianDistributions().get("foo").getAmount() <= 110);
        Assert.assertTrue(madAlgo.getMedianDistributions().get("foo").getChunks().size() <= 12);
    }
}
