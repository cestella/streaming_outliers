package com.caseystella.analytics.kafka;

import com.caseystella.analytics.DataPoint;
import com.caseystella.analytics.extractors.DataPointExtractorConfig;
import com.caseystella.analytics.streaming.outlier.OutlierAlgorithm;
import com.caseystella.analytics.streaming.outlier.OutlierConfig;
import com.caseystella.analytics.streaming.outlier.Severity;
import com.caseystella.analytics.streaming.outlier.mad.SketchyMovingMAD;
import com.caseystella.analytics.streaming.outlier.mad.SketchyMovingMADConfig;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import storm.kafka.Callback;
import storm.kafka.EmitContext;

import java.io.Closeable;
import java.util.List;

public class OutlierCallback implements Callback {
    SketchyMovingMADConfig outlierConfig;
    OutlierAlgorithm outlierAlgorithm;
    public OutlierCallback(SketchyMovingMADConfig outlierConfig) {
        this.outlierConfig = outlierConfig;
    }

    @Override
    public List<Object> apply(List<Object> tuple, EmitContext context) {
        for(Object o : tuple) {
            DataPoint dp = (DataPoint)o;
            Severity severity = outlierAlgorithm.analyze(dp);
            //now what?
        }
        return tuple;
    }

    @Override
    public void initialize(EmitContext context) {
        outlierAlgorithm = new SketchyMovingMAD(outlierConfig);
    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     * <p/>
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     * <p/>
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p/>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p/>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     * <p/>
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p/>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {

    }
}