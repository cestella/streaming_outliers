package com.caseystella.analytics.outlier;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.SimpleTimeRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class InMemoryTSDBTest {
    @Test
    public void testBasics() {
        try {
            InMemoryTimeSeriesDB tsdb = new InMemoryTimeSeriesDB();
            Random r = new Random(0);
            List<DataPoint> points = new ArrayList<>();
            for (int i = 0; i < 100; ++i) {
                double val = r.nextDouble() * 1000;
                DataPoint dp = (new DataPoint(i, val, null, "foo"));
                tsdb.persist("test", dp, new HashMap<String, String>(), null);
                points.add(dp);
            }

            DataPoint evalPt = points.get(50);
            List<DataPoint> retrieved = tsdb.retrieve("test", evalPt, new SimpleTimeRange(0, 100));
            int i = 0;
            Assert.assertEquals(retrieved.size(), 50);
            for (DataPoint foundPt : retrieved) {
                Assert.assertEquals(foundPt, points.get(i++));
            }
        }
        finally {
            InMemoryTimeSeriesDB.clear();
        }
    }
}
