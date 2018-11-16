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
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RFuture;

public class TracingRCountDownLatch extends TracingRObject implements RCountDownLatch {
  private final RCountDownLatch latch;
  private final TracingHelper tracingHelper;

  public TracingRCountDownLatch(RCountDownLatch latch, TracingHelper tracingHelper) {
    super(latch, tracingHelper);
    this.latch = latch;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public void await() throws InterruptedException {
    Span span = tracingHelper.buildSpan("await", latch);
    tracingHelper.decorateThrowing(span, () -> latch.await());
  }

  @Override
  public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("await", latch);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> latch.await(timeout, unit));
  }

  @Override
  public void countDown() {
    Span span = tracingHelper.buildSpan("countDown", latch);
    tracingHelper.decorate(span, latch::countDown);
  }

  @Override
  public long getCount() {
    Span span = tracingHelper.buildSpan("getCount", latch);
    return tracingHelper.decorate(span, latch::getCount);
  }

  @Override
  public boolean trySetCount(long count) {
    Span span = tracingHelper.buildSpan("trySetCount", latch);
    span.setTag("count", count);
    return tracingHelper.decorateThrowing(span, () -> latch.trySetCount(count));
  }

  @Override
  public RFuture<Void> countDownAsync() {
    Span span = tracingHelper.buildSpan("countDownAsync", latch);
    return tracingHelper.prepareRFuture(span, latch::countDownAsync);
  }

  @Override
  public RFuture<Long> getCountAsync() {
    Span span = tracingHelper.buildSpan("getCountAsync", latch);
    return tracingHelper.prepareRFuture(span, latch::getCountAsync);
  }

  @Override
  public RFuture<Boolean> trySetCountAsync(long count) {
    Span span = tracingHelper.buildSpan("trySetCountAsync", latch);
    return tracingHelper.prepareRFuture(span, () -> latch.trySetCountAsync(count));
  }

}
