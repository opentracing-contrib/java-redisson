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
import org.redisson.api.RAtomicLong;
import org.redisson.api.RFuture;

public class TracingRAtomicLong extends TracingRExpirable implements RAtomicLong {
  private final RAtomicLong atomicLong;
  private final TracingHelper tracingHelper;

  public TracingRAtomicLong(RAtomicLong atomicLong, TracingHelper tracingHelper) {
    super(atomicLong, tracingHelper);
    this.atomicLong = atomicLong;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public long getAndDecrement() {
    Span span = tracingHelper.buildSpan("getAndDecrement", atomicLong);
    return tracingHelper.decorate(span, atomicLong::getAndDecrement);
  }

  @Override
  public long addAndGet(long delta) {
    Span span = tracingHelper.buildSpan("addAndGet", atomicLong);
    span.setTag("delta", delta);
    return tracingHelper.decorate(span, () -> atomicLong.addAndGet(delta));
  }

  @Override
  public boolean compareAndSet(long expect, long update) {
    Span span = tracingHelper.buildSpan("compareAndSet", atomicLong);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return tracingHelper.decorate(span, () -> atomicLong.compareAndSet(expect, update));
  }

  @Override
  public long decrementAndGet() {
    Span span = tracingHelper.buildSpan("decrementAndGet", atomicLong);
    return tracingHelper.decorate(span, atomicLong::decrementAndGet);
  }

  @Override
  public long get() {
    Span span = tracingHelper.buildSpan("get", atomicLong);
    return tracingHelper.decorate(span, atomicLong::get);
  }

  @Override
  public long getAndDelete() {
    Span span = tracingHelper.buildSpan("getAndDelete", atomicLong);
    return tracingHelper.decorate(span, atomicLong::getAndDelete);
  }

  @Override
  public long getAndAdd(long delta) {
    Span span = tracingHelper.buildSpan("getAndAdd", atomicLong);
    span.setTag("delta", delta);
    return tracingHelper.decorate(span, () -> atomicLong.getAndAdd(delta));
  }

  @Override
  public long getAndSet(long newValue) {
    Span span = tracingHelper.buildSpan("getAndSet", atomicLong);
    span.setTag("newValue", newValue);
    return tracingHelper.decorate(span, () -> atomicLong.getAndSet(newValue));
  }

  @Override
  public long incrementAndGet() {
    Span span = tracingHelper.buildSpan("incrementAndGet", atomicLong);
    return tracingHelper.decorate(span, atomicLong::incrementAndGet);
  }

  @Override
  public long getAndIncrement() {
    Span span = tracingHelper.buildSpan("getAndIncrement", atomicLong);
    return tracingHelper.decorate(span, atomicLong::getAndIncrement);
  }

  @Override
  public void set(long newValue) {
    Span span = tracingHelper.buildSpan("set", atomicLong);
    span.setTag("newValue", newValue);
    tracingHelper.decorate(span, () -> atomicLong.set(newValue));
  }

  @Override
  public RFuture<Boolean> compareAndSetAsync(long expect, long update) {
    Span span = tracingHelper.buildSpan("compareAndSetAsync", atomicLong);
    span.setTag("expect", expect);
    span.setTag("update", update);
    return tracingHelper.prepareRFuture(span, () -> atomicLong.compareAndSetAsync(expect, update));
  }

  @Override
  public RFuture<Long> addAndGetAsync(long delta) {
    Span span = tracingHelper.buildSpan("addAndGetAsync", atomicLong);
    span.setTag("delta", delta);
    return tracingHelper.prepareRFuture(span, () -> atomicLong.addAndGetAsync(delta));
  }

  @Override
  public RFuture<Long> decrementAndGetAsync() {
    Span span = tracingHelper.buildSpan("decrementAndGetAsync", atomicLong);
    return tracingHelper.prepareRFuture(span, atomicLong::decrementAndGetAsync);
  }

  @Override
  public RFuture<Long> getAsync() {
    Span span = tracingHelper.buildSpan("getAsync", atomicLong);
    return tracingHelper.prepareRFuture(span, atomicLong::getAsync);
  }

  @Override
  public RFuture<Long> getAndDeleteAsync() {
    Span span = tracingHelper.buildSpan("getAndDeleteAsync", atomicLong);
    return tracingHelper.prepareRFuture(span, atomicLong::getAndDeleteAsync);
  }

  @Override
  public RFuture<Long> getAndAddAsync(long delta) {
    Span span = tracingHelper.buildSpan("getAndAddAsync", atomicLong);
    span.setTag("delta", delta);
    return tracingHelper.prepareRFuture(span, () -> atomicLong.getAndAddAsync(delta));
  }

  @Override
  public RFuture<Long> getAndSetAsync(long newValue) {
    Span span = tracingHelper.buildSpan("getAndSetAsync", atomicLong);
    span.setTag("newValue", newValue);
    return tracingHelper.prepareRFuture(span, () -> atomicLong.getAndSetAsync(newValue));
  }

  @Override
  public RFuture<Long> incrementAndGetAsync() {
    Span span = tracingHelper.buildSpan("incrementAndGetAsync", atomicLong);
    return tracingHelper.prepareRFuture(span, atomicLong::incrementAndGetAsync);
  }

  @Override
  public RFuture<Long> getAndIncrementAsync() {
    Span span = tracingHelper.buildSpan("getAndIncrementAsync", atomicLong);
    return tracingHelper.prepareRFuture(span, atomicLong::getAndIncrementAsync);
  }

  @Override
  public RFuture<Long> getAndDecrementAsync() {
    Span span = tracingHelper.buildSpan("getAndDecrementAsync", atomicLong);
    return tracingHelper.prepareRFuture(span, atomicLong::getAndDecrementAsync);
  }

  @Override
  public RFuture<Void> setAsync(long newValue) {
    Span span = tracingHelper.buildSpan("setAsync", atomicLong);
    span.setTag("newValue", newValue);
    return tracingHelper.prepareRFuture(span, () -> atomicLong.setAsync(newValue));
  }

}
