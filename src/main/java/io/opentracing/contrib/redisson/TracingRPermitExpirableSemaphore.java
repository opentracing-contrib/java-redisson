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
import org.redisson.api.RPermitExpirableSemaphore;

public class TracingRPermitExpirableSemaphore extends TracingRExpirable implements
    RPermitExpirableSemaphore {
  private final RPermitExpirableSemaphore semaphore;
  private final TracingHelper tracingHelper;

  public TracingRPermitExpirableSemaphore(RPermitExpirableSemaphore semaphore,
      TracingHelper tracingHelper) {
    super(semaphore, tracingHelper);
    this.semaphore = semaphore;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public String acquire() throws InterruptedException {
    Span span = tracingHelper.buildSpan("acquire", semaphore);
    return tracingHelper.decorateThrowing(span, () -> semaphore.acquire());
  }

  @Override
  public String acquire(long leaseTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("acquire", semaphore);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> semaphore.acquire(leaseTime, unit));
  }

  @Override
  public String tryAcquire() {
    Span span = tracingHelper.buildSpan("tryAcquire", semaphore);
    return tracingHelper.decorate(span, () -> semaphore.tryAcquire());
  }

  @Override
  public String tryAcquire(long waitTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> semaphore.tryAcquire(waitTime, unit));
  }

  @Override
  public String tryAcquire(long waitTime, long leaseTime, TimeUnit unit)
      throws InterruptedException {
    Span span = tracingHelper.buildSpan("tryAcquire", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .decorateThrowing(span, () -> semaphore.tryAcquire(waitTime, leaseTime, unit));
  }

  @Override
  public boolean tryRelease(String permitId) {
    Span span = tracingHelper.buildSpan("tryRelease", semaphore);
    span.setTag("permitId", nullable(permitId));
    return tracingHelper.decorate(span, () -> semaphore.tryRelease(permitId));
  }

  @Override
  public void release(String permitId) {
    Span span = tracingHelper.buildSpan("release", semaphore);
    span.setTag("permitId", nullable(permitId));
    tracingHelper.decorate(span, () -> semaphore.release(permitId));
  }

  @Override
  public int availablePermits() {
    Span span = tracingHelper.buildSpan("availablePermits", semaphore);
    return tracingHelper.decorate(span, semaphore::availablePermits);
  }

  @Override
  public boolean trySetPermits(int permits) {
    Span span = tracingHelper.buildSpan("trySetPermits", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.decorate(span, () -> semaphore.trySetPermits(permits));
  }

  @Override
  public void addPermits(int permits) {
    Span span = tracingHelper.buildSpan("addPermits", semaphore);
    span.setTag("permits", permits);
    tracingHelper.decorate(span, () -> semaphore.addPermits(permits));
  }

  @Override
  public boolean updateLeaseTime(String permitId, long leaseTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("updateLeaseTime", semaphore);
    span.setTag("permitId", permitId);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .decorateThrowing(span, () -> semaphore.updateLeaseTime(permitId, leaseTime, unit));
  }

  @Override
  public RFuture<String> acquireAsync() {
    Span span = tracingHelper.buildSpan("acquireAsync", semaphore);
    return tracingHelper.prepareRFuture(span, semaphore::acquireAsync);
  }

  @Override
  public RFuture<String> acquireAsync(long leaseTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("acquireAsync", semaphore);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> semaphore.acquireAsync(leaseTime, unit));
  }

  @Override
  public RFuture<String> tryAcquireAsync() {
    Span span = tracingHelper.buildSpan("tryAcquireAsync", semaphore);
    return tracingHelper.prepareRFuture(span, semaphore::tryAcquireAsync);
  }

  @Override
  public RFuture<String> tryAcquireAsync(long waitTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> semaphore.tryAcquireAsync(waitTime, unit));
  }

  @Override
  public RFuture<String> tryAcquireAsync(long waitTime, long leaseTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("tryAcquireAsync", semaphore);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .prepareRFuture(span, () -> semaphore.tryAcquireAsync(waitTime, leaseTime, unit));
  }

  @Override
  public RFuture<Boolean> tryReleaseAsync(String permitId) {
    Span span = tracingHelper.buildSpan("tryReleaseAsync", semaphore);
    span.setTag("permitId", permitId);
    return tracingHelper.prepareRFuture(span, () -> semaphore.tryReleaseAsync(permitId));
  }

  @Override
  public RFuture<Void> releaseAsync(String permitId) {
    Span span = tracingHelper.buildSpan("releaseAsync", semaphore);
    span.setTag("permitId", permitId);
    return tracingHelper.prepareRFuture(span, () -> semaphore.releaseAsync(permitId));
  }

  @Override
  public RFuture<Integer> availablePermitsAsync() {
    Span span = tracingHelper.buildSpan("availablePermitsAsync", semaphore);
    return tracingHelper.prepareRFuture(span, semaphore::availablePermitsAsync);
  }

  @Override
  public RFuture<Boolean> trySetPermitsAsync(int permits) {
    Span span = tracingHelper.buildSpan("trySetPermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.prepareRFuture(span, () -> semaphore.trySetPermitsAsync(permits));
  }

  @Override
  public RFuture<Void> addPermitsAsync(int permits) {
    Span span = tracingHelper.buildSpan("addPermitsAsync", semaphore);
    span.setTag("permits", permits);
    return tracingHelper.prepareRFuture(span, () -> semaphore.addPermitsAsync(permits));
  }

  @Override
  public RFuture<Boolean> updateLeaseTimeAsync(String permitId, long leaseTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("updateLeaseTimeAsync", semaphore);
    span.setTag("permitId", permitId);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .prepareRFuture(span, () -> semaphore.updateLeaseTimeAsync(permitId, leaseTime, unit));
  }
}
