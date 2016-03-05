package com.caseystella.analytics.outlier;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.distribution.TimeRange;

public class Outlier {
    DataPoint dataPoint;
    Severity severity;
    TimeRange range;
    Double score;

    public Outlier(DataPoint dataPoint, Severity severity, TimeRange range, Double score) {
        this.dataPoint = dataPoint;
        this.severity = severity;
        this.range = range;
        this.score = score;
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
