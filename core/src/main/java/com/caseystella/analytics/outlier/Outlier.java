package com.caseystella.analytics.outlier;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.TimeRange;
import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Outlier {
    DataPoint dataPoint;
    Severity severity;
    TimeRange range;
    Double score;
    int numPts;

    public Outlier(DataPoint dataPoint, Severity severity, TimeRange range, Double score, int numPts) {
        this.dataPoint = dataPoint;
        this.severity = severity;
        this.range = range;
        this.score = score;
        this.numPts = numPts;
    }

    public static Map<String, String> groupingFilter(DataPoint dp, List<String> groupingKeys) {
        Map<String, String> filter = new HashMap<>();
        if(groupingKeys != null) {
            for (String gk : groupingKeys) {
                String k = dp.getMetadata().get(gk);
                if (k != null) {
                    filter.put(gk, k);
                }
            }
        }
        return filter;
    }

    public static String groupingKey(DataPoint dp, List<String> groupingKeys) {
        List<String> keyParts = new ArrayList<>();
        keyParts.add(dp.getSource());
        if(groupingKeys != null) {
            for (String gk : groupingKeys) {
                String k = dp.getMetadata().get(gk);
                if (k != null) {
                    keyParts.add(k);
                }
            }
        }
        return Joiner.on('_').join(keyParts);
    }

    public int getNumPts() {
        return numPts;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    public DataPoint getDataPoint() {
        return dataPoint;
    }

    public void setDataPoint(DataPoint dataPoint) {
        this.dataPoint = dataPoint;
    }

    public Severity getSeverity() {
        return severity;
    }

    public void setSeverity(Severity severity) {
        this.severity = severity;
    }

    public TimeRange getRange() {
        return range;
    }

    public void setRange(TimeRange range) {
        this.range = range;
    }

    @Override
    public String toString() {
        return "Outlier{" +
                "dataPoint=" + dataPoint +
                ", severity=" + severity +
                ", range=" + range +
                ", score=" + score +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Outlier outlier = (Outlier) o;

        if (getDataPoint() != null ? !getDataPoint().equals(outlier.getDataPoint()) : outlier.getDataPoint() != null)
            return false;
        if (getSeverity() != outlier.getSeverity()) return false;
        if (getRange() != null ? !getRange().equals(outlier.getRange()) : outlier.getRange() != null) return false;
        return getScore() != null ? getScore().equals(outlier.getScore()) : outlier.getScore() == null;

    }

    @Override
    public int hashCode() {
        int result = getDataPoint() != null ? getDataPoint().hashCode() : 0;
        result = 31 * result + (getSeverity() != null ? getSeverity().hashCode() : 0);
        result = 31 * result + (getRange() != null ? getRange().hashCode() : 0);
        result = 31 * result + (getScore() != null ? getScore().hashCode() : 0);
        return result;
    }
}
