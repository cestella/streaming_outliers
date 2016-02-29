package com.caseystella.analytics.distribution;

import com.caseystella.analytics.DataPoint;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.twitter.algebird.QTree;
import scala.Tuple2;

import java.util.LinkedList;

public class Distribution implements Measurable {
    public static class Context {
        private Distribution currentDistribution;
        private LinkedList<Distribution> chunks = new LinkedList<>();

        public Distribution getCurrentDistribution() {
            return currentDistribution;
        }
        public LinkedList<Distribution> getChunks() {
            return chunks;
        }
        public long getAmount() {
            return currentDistribution == null?0L:currentDistribution.getAmount();
        }

        public void addDataPoint(DataPoint dp, RotationConfig rotationPolicy, RotationConfig chunkingPolicy) {
            if(currentDistribution == null) {
                currentDistribution = new Distribution(dp);
            }
            else {
                currentDistribution.addDataPoint(dp);
            }
            //do I need to create a new chunk?
            boolean needNewChunk = chunks.size() == 0 || outOfPolicy(getCurrentChunk(), chunkingPolicy);
            if(needNewChunk) {
                addChunk(new Distribution(dp));
            }
            else {
                getCurrentChunk().addDataPoint(dp);
            }
            if(needNewChunk) {
                //do I need to rotate now?
                boolean needsRotation = outOfPolicy(currentDistribution, rotationPolicy)
                                    && outOfPolicy(sketch(Iterables.limit(chunks, chunks.size() - 1)), rotationPolicy);
                if(needsRotation) {
                    rotate();
                }
            }
        }

        protected void addChunk(Distribution d) {
            chunks.addFirst(d);
        }

        protected void rotate() {
            chunks.removeLast();
            currentDistribution = Distribution.merge(chunks);
        }

        private Distribution getCurrentChunk() {
            return chunks.getFirst();
        }

        private Measurable sketch(Iterable<Distribution> chunks) {
            long begin = Long.MAX_VALUE;
            long end = -1;
            long amount = 0;
            for(Distribution d : chunks) {
                begin = Math.min(begin, d.getBegin());
                end = Math.max(end, d.getEnd());
                amount += d.getAmount();
            }
            final long measurableBegin = begin;
            final long measurableEnd= end;
            final long measurableAmount = amount;
            return new Measurable() {
                @Override
                public long getAmount() {
                    return measurableAmount;
                }

                @Override
                public Long getBegin() {
                    return measurableBegin;
                }

                @Override
                public Long getEnd() {
                    return measurableEnd;
                }
            };
        }

        private boolean outOfPolicy(Measurable dist, RotationConfig policy) {
            if(policy.getType() == Type.BY_AMOUNT) {
                return dist.getAmount() >= policy.getAmount();
            }
            else if(policy.getType() == Type.BY_TIME) {
                return dist.getAmount() >= policy.getUnit().apply(dist);
            }
            else if(policy.getType() == Type.NEVER) {
                return false;
            }
            else {
                throw new IllegalStateException("Unsupported type: " + policy.getType());
            }
        }



    }
    QTree<Object> distribution;
    long begin;
    long end;
    long amount;

    public Distribution(QTree<Object> distribution, long begin, long end, long amount) {
        this.distribution = distribution;
        this.begin = begin;
        this.end = end;
        this.amount = amount;
    }
    public Distribution(DataPoint dp) {
        this.begin = dp.getTimestamp();
        this.end = dp.getTimestamp();
        this.amount = 1L;
        this.distribution = DistributionUtils.createTree(ImmutableList.of(dp.getValue()));
    }

    public void addDataPoint(DataPoint dp ) {
        this.end = Math.max(end, dp.getTimestamp());
        this.begin = Math.min(begin, dp.getTimestamp());
        this.amount++;
        this.distribution = DistributionUtils.merge(this.distribution, DistributionUtils.createTree(ImmutableList.of(dp.getValue())));
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
    public long getAmount() {
        return amount;
    }

    public static Distribution merge(Iterable<Distribution> distributions) {
        QTree<Object> distribution = null;
        long begin = Long.MAX_VALUE;
        long end = -1l;
        long amount = 0l;
        for(Distribution d : distributions) {
            if(distribution == null) {
                distribution = d.distribution;
            }
            else {
                distribution = DistributionUtils.merge(distribution, d.distribution);
            }
            begin = Math.min(begin, d.begin);
            end = Math.max(end, d.end);
            amount += d.amount;
        }
        return new Distribution(distribution, begin, end, amount);
    }

    public ValueRange getPercentileRange(double percentile) {
        Tuple2<Object, Object> bounds = distribution.quantileBounds(percentile);
        if(Math.abs(percentile - 0.0) < 1e-6) {
            double min = ((Number)bounds._1()).doubleValue();
            return new ValueRange(min, min);
        }
        if(Math.abs(percentile - 1.0) < 1e-6) {
            double max = ((Number)bounds._2()).doubleValue();
            return new ValueRange(max, max);
        }
        double l = ((Number)bounds._1()).doubleValue();
        double r = ((Number)bounds._2()).doubleValue();
        return new ValueRange(l, r);
    }

    public double getPercentileRange(double percentile, Function<Range<Double>, Double> approximator) {
        return approximator.apply(getPercentileRange(percentile));
    }

    public double getMedian(Function<Range<Double>, Double> approximator) {
        return getPercentileRange(0.5, approximator);
    }

    public double getMedian() {
        return getMedian(RangeApproximator.MIDPOINT);
    }

}
