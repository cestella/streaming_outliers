package com.caseystella.analytics.cli;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.extractor.DataPointExtractor;
import com.caseystella.analytics.extractor.DataPointExtractorConfig;
import com.caseystella.analytics.outlier.Outlier;
import com.caseystella.analytics.outlier.Severity;
import com.caseystella.analytics.outlier.batch.OutlierAlgorithm;
import com.caseystella.analytics.outlier.batch.rpca.RPCAOutlierAlgorithm;
import com.caseystella.analytics.outlier.streaming.mad.SketchyMovingMAD;
import com.caseystella.analytics.timeseries.inmemory.InMemoryTimeSeriesDB;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DryRun {
    public static final String METRIC = "metric";
    DataPointExtractorConfig extractorConfig ;
    com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig;
    public DryRun( DataPointExtractorConfig extractorConfig
                 , com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig
                 )
    {
        this.extractorConfig = extractorConfig;
        this.streamingOutlierConfig = streamingOutlierConfig;
    }

    public void run(File inputFile, File tsOutF, File sketchyOutF, File realOutF) throws IOException
    {
        PrintWriter tsOut = new PrintWriter(tsOutF)
                  , sketchyOut = new PrintWriter(sketchyOutF)
                  , realOut = new PrintWriter(realOutF);
        SketchyMovingMAD madAlgo = ((SketchyMovingMAD)streamingOutlierConfig.getSketchyOutlierAlgorithm())
                                                                            .withConfig(streamingOutlierConfig);

        OutlierAlgorithm detector =  streamingOutlierConfig.getBatchOutlierAlgorithm();
        detector.configure(streamingOutlierConfig);
        DataPointExtractor extractor = new DataPointExtractor().withConfig(extractorConfig);
        InMemoryTimeSeriesDB tsdb = new InMemoryTimeSeriesDB();
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        int lineNo = 1;
        for(String line = null;(line = br.readLine()) != null;lineNo++) {
            if(lineNo % 100 == 0) {
                System.out.print(".");
            }
            if(lineNo % 1000 == 0) {
                System.out.println(" -- Completed " + lineNo);
            }
            for(DataPoint dp : extractor.extract(new byte[]{}, Bytes.toBytes(line), false)) {
                String pt = dp.getTimestamp() + "," + dp.getValue();
                tsdb.persist("metric", dp, new HashMap<String, String>(), null);
                tsOut.println(pt);
                Outlier outlier = madAlgo.analyze(dp);
                if(outlier.getSeverity() == Severity.SEVERE_OUTLIER) {
                    sketchyOut.println(pt);
                    Outlier realOutlier = detector.analyze(outlier, outlier.getSample(), dp);
                    if(realOutlier.getSeverity() == Severity.SEVERE_OUTLIER) {
                        realOut.println(pt);
                    }
                }
            }
        }
        tsOut.close();
        sketchyOut.close();
        realOut.close();
    }
}
