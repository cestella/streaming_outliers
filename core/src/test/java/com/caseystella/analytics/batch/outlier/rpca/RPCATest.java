package com.caseystella.analytics.batch.outlier.rpca;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.batch.rpca.RPCAOutlierAlgorithm;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RPCATest {
  @Test
  public void test() throws Exception {
    Random r = new Random(0);
    List<DataPoint> points = new ArrayList<>();
    for(int i = 0; i < 100;++i) {
      double val = r.nextDouble()*1000;
      DataPoint dp = (new DataPoint(i, val, null, "foo"));
      points.add(dp);
    }
    DataPoint evaluationPoint = new DataPoint(101, 10000, null, "foo");
    RPCAOutlierAlgorithm detector = new RPCAOutlierAlgorithm();
    detector.isOutlier(points, evaluationPoint);
  }
}
