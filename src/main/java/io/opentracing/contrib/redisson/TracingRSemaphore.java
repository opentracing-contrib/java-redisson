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
import org.redisson.api.RFuture;
import org.redisson.api.RSemaphore;

public class TracingRSemaphore extends TracingRExpirable implements RSemaphore {
  private final RSemaphore semaphore;
  private final TracingHelper tracingHelper;

  public TracingRSemaphore(RSemaphore semaphore, TracingHelper tracingHelper) {
    super(semaphore, tracingHelper);
    this.semaphore = semaphore;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public void acquire() throws InterruptedException {
    Span span = tracingHelper.buildSpan("acquire", semaphore);
    tracingHelper.decorateThrowing(span, () -> semaphore.acquire());
  }

  @Override
  public void acquire(int permits) throws InterruptedException {
    Span span = tracingHelper.buildSpan("acquire", semaphore);
    span.setTag("permits", permits);
    tracingHelper.decorateThrowing(span, () -> semaphore.acquire(permits));
  }

  @Override
  public boolean tryAcquire() {
    Span span = tracingHelper.buildSpan("tryAcquire", semaphore);
    return tracingHelper.decorate(span, () -> semaphore.tryAcquire());
  }

  @Override
  public boolean tryAcquire(int permits) {
    Span span = tracingHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.decorate(span, () -> semaphore.tryAcquire(permits));
  }

  @Override
  public boolean tryAcquire(long waitTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> semaphore.tryAcquire(waitTime, unit));
  }

  @Override
  public boolean tryAcquire(int permits, long waitTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("permits", permits);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .decorateThrowing(span, () -> semaphore.tryAcquire(permits, waitTime, unit));
  }

  @Override
  public void release() {
    Span span = tracingHelper.buildSpan("release", semaphore);
    tracingHelper.decorate(span, () -> semaphore.release());
  }

  @Override
  public void release(int permits) {
    Span span = tracingHelper.buildSpan("release", semaphore);
    span.setTag("permits", permits);
    tracingHelper.decorate(span, () -> semaphore.release(permits));
  }

  @Override
  public int availablePermits() {
    Span span = tracingHelper.buildSpan("availablePermits", semaphore);
    return tracingHelper.decorate(span, semaphore::availablePermits);
  }

  @Override
  public int drainPermits() {
    Span span = tracingHelper.buildSpan("drainPermits", semaphore);
    return tracingHelper.decorate(span, semaphore::drainPermits);
  }

  @Override
  public boolean trySetPermits(int permits) {
    Span span = tracingHelper.buildSpan("trySetPermits", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.decorate(span, () -> semaphore.trySetPermits(permits));
  }

  @Override
  public void reducePermits(int permits) {
    Span span = tracingHelper.buildSpan("reducePermits", semaphore);
    span.setTag("permits", permits);
    tracingHelper.decorate(span, () -> semaphore.reducePermits(permits));
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync() {
    Span span = tracingHelper.buildSpan("tryAcquireAsync", semaphore);
    return tracingHelper.prepareRFuture(span, semaphore::tryAcquireAsync);
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync(int permits) {
    Span span = tracingHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.prepareRFuture(span, () -> semaphore.tryAcquireAsync(permits));
  }

  @Override
  public RFuture<Void> acquireAsync() {
    Span span = tracingHelper.buildSpan("acquireAsync", semaphore);
    return tracingHelper.prepareRFuture(span, semaphore::acquireAsync);
  }

  @Override
  public RFuture<Void> acquireAsync(int permits) {
    Span span = tracingHelper.buildSpan("acquireAsync", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.prepareRFuture(span, () -> semaphore.acquireAsync(permits));
  }

  @Override
  public RFuture<Void> releaseAsync() {
    Span span = tracingHelper.buildSpan("releaseAsync", semaphore);
    return tracingHelper.prepareRFuture(span, semaphore::releaseAsync);
  }

  @Override
  public RFuture<Void> releaseAsync(int permits) {
    Span span = tracingHelper.buildSpan("releaseAsync", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.prepareRFuture(span, () -> semaphore.releaseAsync(permits));
  }

  @Override
  public RFuture<Boolean> trySetPermitsAsync(int permits) {
    Span span = tracingHelper.buildSpan("trySetPermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.prepareRFuture(span, () -> semaphore.trySetPermitsAsync(permits));
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync(long waitTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> semaphore.tryAcquireAsync(waitTime, unit));
  }

  @Override
  public RFuture<Boolean> tryAcquireAsync(int permits, long waitTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("permits", permits);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .prepareRFuture(span, () -> semaphore.tryAcquireAsync(permits, waitTime, unit));
  }

  @Override
  public RFuture<Void> reducePermitsAsync(int permits) {
    Span span = tracingHelper.buildSpan("reducePermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.prepareRFuture(span, () -> semaphore.reducePermitsAsync(permits));
  }

}
