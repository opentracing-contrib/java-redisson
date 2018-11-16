/*
 * Copyright 2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.redisson;

import static io.opentracing.contrib.redisson.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RDoubleAdder;
import org.redisson.api.RFuture;

public class TracingRDoubleAdder extends TracingRExpirable implements RDoubleAdder {
  private final RDoubleAdder doubleAdder;
  private final TracingHelper tracingHelper;

  public TracingRDoubleAdder(RDoubleAdder doubleAdder, TracingHelper tracingHelper) {
    super(doubleAdder, tracingHelper);
    this.doubleAdder = doubleAdder;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public void add(double x) {
    Span span = tracingHelper.buildSpan("add", doubleAdder);
    span.setTag("value", x);
    tracingHelper.decorate(span, () -> doubleAdder.add(x));
  }

  @Override
  public void increment() {
    Span span = tracingHelper.buildSpan("increment", doubleAdder);
    tracingHelper.decorate(span, doubleAdder::increment);
  }

  @Override
  public void decrement() {
    Span span = tracingHelper.buildSpan("decrement", doubleAdder);
    tracingHelper.decorate(span, doubleAdder::decrement);
  }

  @Override
  public double sum() {
    Span span = tracingHelper.buildSpan("sum", doubleAdder);
    return tracingHelper.decorate(span, doubleAdder::sum);
  }

  @Override
  public void reset() {
    Span span = tracingHelper.buildSpan("reset", doubleAdder);
    tracingHelper.decorate(span, doubleAdder::reset);
  }

  @Override
  public RFuture<Double> sumAsync() {
    Span span = tracingHelper.buildSpan("sumAsync", doubleAdder);
    return tracingHelper.prepareRFuture(span, doubleAdder::sumAsync);
  }

  @Override
  public RFuture<Double> sumAsync(long timeout, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("sumAsync", doubleAdder);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper.prepareRFuture(span, () -> doubleAdder.sumAsync(timeout, timeUnit));
  }

  @Override
  public RFuture<Void> resetAsync() {
    Span span = tracingHelper.buildSpan("resetAsync", doubleAdder);
    return tracingHelper.prepareRFuture(span, doubleAdder::resetAsync);
  }

  @Override
  public RFuture<Void> resetAsync(long timeout, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("resetAsync", doubleAdder);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper.prepareRFuture(span, () -> doubleAdder.resetAsync(timeout, timeUnit));
  }

  @Override
  public void destroy() {
    Span span = tracingHelper.buildSpan("destroy", doubleAdder);
    tracingHelper.decorate(span, doubleAdder::destroy);
  }

}
