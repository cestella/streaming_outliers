package com.caseystella.analytics.distribution;

public class SimpleTimeRange implements TimeRange{
    private long begin;
    private long end;
    public SimpleTimeRange(TimeRange tr) {
        this.begin = tr.getBegin();
        this.end = tr.getEnd();
    }

    @Override
    public Long getBegin() {
        return begin;
    }

    @Override
    public Long getEnd() {
        return end;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleTimeRange that = (SimpleTimeRange) o;

        if (getBegin() != that.getBegin()) return false;
        return getEnd() == that.getEnd();

    }

    @Override
    public int hashCode() {
        int result = (int) (getBegin() ^ (getBegin() >>> 32));
        result = 31 * result + (int) (getEnd() ^ (getEnd() >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "(" + begin +
                "," + end +
                ')';
    }
}
