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

import io.opentracing.Span;
import org.redisson.api.RAtomicDouble;
import org.redisson.api.RFuture;

public class TracingRAtomicDouble extends TracingRExpirable implements RAtomicDouble {
  private final RAtomicDouble atomicDouble;
  private final TracingHelper tracingHelper;

  public TracingRAtomicDouble(RAtomicDouble atomicDouble, TracingHelper tracingHelper) {
    super(atomicDouble, tracingHelper);
    this.atomicDouble = atomicDouble;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public double getAndDecrement() {
    Span span = tracingHelper.buildSpan("getAndDecrement", atomicDouble);
    return tracingHelper.decorate(span, atomicDouble::getAndDecrement);
  }

  @Override
  public double addAndGet(double delta) {
    Span span = tracingHelper.buildSpan("addAndGet", atomicDouble);
    span.setTag("delta", delta);
    return tracingHelper.decorate(span, () -> atomicDouble.addAndGet(delta));
  }

  @Override
  public boolean compareAndSet(double expect, double update) {
    Span span = tracingHelper.buildSpan("compareAndSet", atomicDouble);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return tracingHelper.decorate(span, () -> atomicDouble.compareAndSet(expect, update));
  }

  @Override
  public double decrementAndGet() {
    Span span = tracingHelper.buildSpan("decrementAndGet", atomicDouble);
    return tracingHelper.decorate(span, atomicDouble::decrementAndGet);
  }

  @Override
  public double get() {
    Span span = tracingHelper.buildSpan("get", atomicDouble);
    return tracingHelper.decorate(span, atomicDouble::get);
  }

  @Override
  public double getAndDelete() {
    Span span = tracingHelper.buildSpan("getAndDelete", atomicDouble);
    return tracingHelper.decorate(span, atomicDouble::getAndDelete);
  }

  @Override
  public double getAndAdd(double delta) {
    Span span = tracingHelper.buildSpan("getAndAdd", atomicDouble);
    span.setTag("delta", delta);
    return tracingHelper.decorate(span, () -> atomicDouble.getAndAdd(delta));
  }

  @Override
  public double getAndSet(double newValue) {
    Span span = tracingHelper.buildSpan("getAndSet", atomicDouble);
    span.setTag("newValue", newValue);
    return tracingHelper.decorate(span, () -> atomicDouble.getAndSet(newValue));
  }

  @Override
  public double incrementAndGet() {
    Span span = tracingHelper.buildSpan("incrementAndGet", atomicDouble);
    return tracingHelper.decorate(span, atomicDouble::incrementAndGet);
  }

  @Override
  public double getAndIncrement() {
    Span span = tracingHelper.buildSpan("getAndIncrement", atomicDouble);
    return tracingHelper.decorate(span, atomicDouble::getAndIncrement);
  }

  @Override
  public void set(double newValue) {
    Span span = tracingHelper.buildSpan("set", atomicDouble);
    span.setTag("newValue", newValue);
    tracingHelper.decorate(span, () -> atomicDouble.set(newValue));
  }

  @Override
  public RFuture<Boolean> compareAndSetAsync(double expect, double update) {
    Span span = tracingHelper.buildSpan("compareAndSetAsync", atomicDouble);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return tracingHelper
        .prepareRFuture(span, () -> atomicDouble.compareAndSetAsync(expect, update));
  }

  @Override
  public RFuture<Double> addAndGetAsync(double delta) {
    Span span = tracingHelper.buildSpan("addAndGetAsync", atomicDouble);
    span.setTag("delta", delta);
    return tracingHelper.prepareRFuture(span, () -> atomicDouble.addAndGetAsync(delta));
  }

  @Override
  public RFuture<Double> decrementAndGetAsync() {
    Span span = tracingHelper.buildSpan("decrementAndGetAsync", atomicDouble);
    return tracingHelper.prepareRFuture(span, atomicDouble::decrementAndGetAsync);
  }

  @Override
  public RFuture<Double> getAsync() {
    Span span = tracingHelper.buildSpan("getAsync", atomicDouble);
    return tracingHelper.prepareRFuture(span, atomicDouble::getAsync);
  }

  @Override
  public RFuture<Double> getAndDeleteAsync() {
    Span span = tracingHelper.buildSpan("getAndDeleteAsync", atomicDouble);
    return tracingHelper.prepareRFuture(span, atomicDouble::getAndDeleteAsync);
  }

  @Override
  public RFuture<Double> getAndAddAsync(double delta) {
    Span span = tracingHelper.buildSpan("getAndAddAsync", atomicDouble);
    span.setTag("delta", delta);
    return tracingHelper.prepareRFuture(span, () -> atomicDouble.getAndAddAsync(delta));
  }

  @Override
  public RFuture<Double> getAndSetAsync(double newValue) {
    Span span = tracingHelper.buildSpan("getAndSetAsync", atomicDouble);
    span.setTag("newValue", newValue);
    return tracingHelper.prepareRFuture(span, () -> atomicDouble.getAndSetAsync(newValue));
  }

  @Override
  public RFuture<Double> incrementAndGetAsync() {
    Span span = tracingHelper.buildSpan("incrementAndGetAsync", atomicDouble);
    return tracingHelper.prepareRFuture(span, atomicDouble::incrementAndGetAsync);
  }

  @Override
  public RFuture<Double> getAndIncrementAsync() {
    Span span = tracingHelper.buildSpan("getAndIncrementAsync", atomicDouble);
    return tracingHelper.prepareRFuture(span, atomicDouble::getAndIncrementAsync);
  }

  @Override
  public RFuture<Double> getAndDecrementAsync() {
    Span span = tracingHelper.buildSpan("getAndDecrementAsync", atomicDouble);
    return tracingHelper.prepareRFuture(span, atomicDouble::getAndDecrementAsync);
  }

  @Override
  public RFuture<Void> setAsync(double newValue) {
    Span span = tracingHelper.buildSpan("setAsync", atomicDouble);
    span.setTag("newValue", newValue);
    return tracingHelper.prepareRFuture(span, () -> atomicDouble.setAsync(newValue));
  }

}
