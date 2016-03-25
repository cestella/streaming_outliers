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
import java.util.*;

public class DryRun {
    public static final String METRIC = "metric";
    DataPointExtractorConfig extractorConfig ;
    com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig;
    Map<String, String> outputFilter;
    public DryRun( DataPointExtractorConfig extractorConfig
                 , com.caseystella.analytics.outlier.streaming.OutlierConfig streamingOutlierConfig
                 , Properties outputFilter
                 )
    {
        this.extractorConfig = extractorConfig;
        this.streamingOutlierConfig = streamingOutlierConfig;
        this.outputFilter = new HashMap<>();
        for(Map.Entry<Object, Object> kv : outputFilter.entrySet()) {
            this.outputFilter.put(kv.getKey().toString(), kv.getValue().toString());
        }
    }
    public boolean filterMatch(DataPoint dp) {
        boolean ret = true;
        for(Map.Entry<String, String> kv : outputFilter.entrySet()) {
            String target = dp.getMetadata().get(kv.getKey());
            ret &= target != null && target.toLowerCase().startsWith(kv.getValue().toLowerCase());
        }
        return ret;
    }
    public void run(File inputFile, File tsOutF, File sketchyOutF, File realOutF) throws IOException
    {
        System.out.println("Filter: " + this.outputFilter);
        PrintWriter tsOut = new PrintWriter(tsOutF)
                  , sketchyOut = new PrintWriter(sketchyOutF)
                  , realOut = new PrintWriter(realOutF);
        SketchyMovingMAD madAlgo = ((SketchyMovingMAD)streamingOutlierConfig.getSketchyOutlierAlgorithm())
                                                                            .withConfig(streamingOutlierConfig);

        OutlierAlgorithm detector =  streamingOutlierConfig.getBatchOutlierAlgorithm();
        detector.configure(streamingOutlierConfig);
        DataPointExtractor extractor = new DataPointExtractor().withConfig(extractorConfig);
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
                boolean print = true;
                if(!outputFilter.isEmpty()) {
                    print = filterMatch(dp);
                }
                if(print) {
                    tsOut.println(pt);
                }
                Outlier outlier = madAlgo.analyze(dp);
                if(outlier.getSeverity() == Severity.SEVERE_OUTLIER) {
                    if(print) {
                        sketchyOut.println(pt);
                    }
                    Outlier realOutlier = detector.analyze(outlier, outlier.getSample(), dp);
                    if(realOutlier.getSeverity() == Severity.SEVERE_OUTLIER) {
                        if(print) {
                            realOut.println(pt);
                        }
                    }
                }
            }
        }
        tsOut.close();
        sketchyOut.close();
        realOut.close();
    }
}
