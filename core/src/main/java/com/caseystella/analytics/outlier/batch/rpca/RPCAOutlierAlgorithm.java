package com.caseystella.analytics.outlier.batch.rpca;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.outlier.batch.OutlierAlgorithm;
import com.caseystella.analytics.outlier.batch.OutlierConfig;

import java.util.List;

public class RPCAOutlierAlgorithm implements OutlierAlgorithm{
    private static final double EPSILON = 1e-12;
    private final double LPENALTY_DEFAULT = 1;
    private final double SPENALTY_DEFAULT = 1.4;
    private Double  lpenalty;
    private Double  spenalty;
    private Boolean isForceDiff = false;
    private int minRecords = 0;

    public RPCAOutlierAlgorithm() {

    }



    // Helper Function
    public double[][] VectorToMatrix(double[] x, int rows, int cols) {
        double[][] input2DArray = new double[rows][cols];
        for (int n= 0; n< x.length; n++) {
            int i = n % rows;
            int j = (int) Math.floor(n / rows);
            input2DArray[i][j] = x[n];
        }
        return input2DArray;
    }

    public Severity isOutlier(List<DataPoint> dataPoints, DataPoint value) {
        double[] inputData = new double[dataPoints.size() + 1];
        int i = 0;
        int numNonZero = 0;
        for(DataPoint dp : dataPoints) {
            inputData[i++] = dp.getValue();
            numNonZero += dp.getValue() > EPSILON?1:0;
        }
        inputData[i] = value.getValue();
        int nCols = 1;
        int nRows = inputData.length;
        if(numNonZero > minRecords) {
            AugmentedDickeyFuller dickeyFullerTest = new AugmentedDickeyFuller(inputData);
            double[] inputArrayTransformed = inputData;
            if (this.isForceDiff == null && dickeyFullerTest.isNeedsDiff()) {
                // Auto Diff
                inputArrayTransformed = dickeyFullerTest.getZeroPaddedDiff();
            } else if (this.isForceDiff) {
                // Force Diff
                inputArrayTransformed = dickeyFullerTest.getZeroPaddedDiff();
            }

            if (this.spenalty == null) {
                this.lpenalty = this.LPENALTY_DEFAULT;
                this.spenalty = this.SPENALTY_DEFAULT/ Math.sqrt(Math.max(nCols, nRows));
            }


            // Calc Mean
            double mean  = 0;
            for (int n=0; n < inputArrayTransformed.length; n++) {
                mean += inputArrayTransformed[n];
            }
            mean /= inputArrayTransformed.length;

            // Calc STDEV
            double stdev = 0;
            for (int n=0; n < inputArrayTransformed.length; n++) {
                stdev += Math.pow(inputArrayTransformed[n] - mean,2) ;
            }
            stdev = Math.sqrt(stdev / (inputArrayTransformed.length - 1));

            // Transformation: Zero Mean, Unit Variance
            for (int n=0; n < inputArrayTransformed.length; n++) {
                inputArrayTransformed[n] = (inputArrayTransformed[n]-mean)/stdev;
            }

            // Read Input Data into Array
            // Read Input Data into Array
            double[][] input2DArray = new double[nRows][nCols];
            input2DArray = VectorToMatrix(inputArrayTransformed, nRows, nCols);

            RPCA rSVD = new RPCA(input2DArray, this.lpenalty, this.spenalty);

            double[][] outputE = rSVD.getE().getData();
            double[][] outputS = rSVD.getS().getData();
            double[][] outputL = rSVD.getL().getData();
            double E = outputE[nRows-1][0];
            double S = outputS[nRows-1][0];
            double L = outputL[nRows-1][0];
            return Math.abs(S) > 0?Severity.SEVERE_OUTLIER:Severity.NORMAL;
        }
        else {
            return Severity.NOT_ENOUGH_DATA;
        }
    }

    @Override
    public Outlier analyze(Outlier outlierCandidate, List<DataPoint> context, DataPoint dp) {
        outlierCandidate.setSeverity(isOutlier(context, dp));
        return outlierCandidate;
    }

    @Override
    public void configure(OutlierConfig configStr) {

    }
}
