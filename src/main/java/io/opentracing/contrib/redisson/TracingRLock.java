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
import java.util.concurrent.locks.Condition;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;

public class TracingRLock extends TracingRExpirable implements RLock {
  private final RLock lock;
  private final TracingHelper tracingHelper;

  public TracingRLock(RLock lock, TracingHelper tracingHelper) {
    super(lock, tracingHelper);
    this.lock = lock;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public void lockInterruptibly(long leaseTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("lockInterruptibly", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    tracingHelper.decorateThrowing(span, () -> lock.lockInterruptibly(leaseTime, unit));
  }

  @Override
  public boolean tryLock(long waitTime, long leaseTime, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("tryLock", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> lock.tryLock(waitTime, leaseTime, unit));
  }

  @Override
  public void lock(long leaseTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("lock", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    tracingHelper.decorate(span, () -> lock.lock(leaseTime, unit));
  }

  @Override
  public boolean forceUnlock() {
    Span span = tracingHelper.buildSpan("forceUnlock", lock);
    return tracingHelper.decorate(span, lock::forceUnlock);
  }

  @Override
  public boolean isLocked() {
    Span span = tracingHelper.buildSpan("isLocked", lock);
    return tracingHelper.decorate(span, lock::isLocked);
  }

  @Override
  public boolean isHeldByCurrentThread() {
    Span span = tracingHelper.buildSpan("isHeldByCurrentThread", lock);
    return tracingHelper.decorate(span, lock::isHeldByCurrentThread);
  }

  @Override
  public int getHoldCount() {
    Span span = tracingHelper.buildSpan("getHoldCount", lock);
    return tracingHelper.decorate(span, lock::getHoldCount);
  }

  @Override
  public void lock() {
    Span span = tracingHelper.buildSpan("lock", lock);
    tracingHelper.decorate(span, () -> lock.lock());
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    Span span = tracingHelper.buildSpan("lockInterruptibly", lock);
    tracingHelper.decorateThrowing(span, () -> lock.lockInterruptibly());
  }

  @Override
  public boolean tryLock() {
    Span span = tracingHelper.buildSpan("tryLock", lock);
    return tracingHelper.decorate(span, () -> lock.tryLock());
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("tryLock", lock);
    span.setTag("time", time);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> lock.tryLock(time, unit));
  }

  @Override
  public void unlock() {
    Span span = tracingHelper.buildSpan("unlock", lock);
    tracingHelper.decorate(span, lock::unlock);
  }

  @Override
  public Condition newCondition() {
    Span span = tracingHelper.buildSpan("newCondition", lock);
    return tracingHelper.decorate(span, lock::newCondition);
  }

  @Override
  public RFuture<Boolean> forceUnlockAsync() {
    Span span = tracingHelper.buildSpan("forceUnlockAsync", lock);
    return tracingHelper.prepareRFuture(span, lock::forceUnlockAsync);
  }

  @Override
  public RFuture<Void> unlockAsync() {
    Span span = tracingHelper.buildSpan("unlockAsync", lock);
    return tracingHelper.prepareRFuture(span, lock::unlockAsync);
  }

  @Override
  public RFuture<Void> unlockAsync(long threadId) {
    Span span = tracingHelper.buildSpan("unlockAsync", lock);
    span.setTag("threadId", threadId);
    return tracingHelper.prepareRFuture(span, () -> lock.unlockAsync(threadId));
  }

  @Override
  public RFuture<Boolean> tryLockAsync() {
    Span span = tracingHelper.buildSpan("tryLockAsync", lock);
    return tracingHelper.prepareRFuture(span, lock::tryLockAsync);
  }

  @Override
  public RFuture<Void> lockAsync() {
    Span span = tracingHelper.buildSpan("lockAsync", lock);
    return tracingHelper.prepareRFuture(span, lock::lockAsync);
  }

  @Override
  public RFuture<Void> lockAsync(long threadId) {
    Span span = tracingHelper.buildSpan("lockAsync", lock);
    span.setTag("threadId", threadId);
    return tracingHelper.prepareRFuture(span, () -> lock.lockAsync(threadId));
  }

  @Override
  public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("lockAsync", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> lock.lockAsync(leaseTime, unit));
  }

  @Override
  public RFuture<Void> lockAsync(long leaseTime, TimeUnit unit,
      long threadId) {
    Span span = tracingHelper.buildSpan("lockAsync", lock);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    span.setTag("threadId", threadId);
    return tracingHelper.prepareRFuture(span, () -> lock.lockAsync(leaseTime, unit, threadId));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long threadId) {
    Span span = tracingHelper.buildSpan("tryLockAsync", lock);
    span.setTag("threadId", threadId);
    return tracingHelper.prepareRFuture(span, () -> lock.tryLockAsync(threadId));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("tryLockAsync", lock);
    span.setTag("waitTime", waitTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> lock.tryLockAsync(waitTime, unit));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("tryLockAsync", lock);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> lock.tryLockAsync(waitTime, leaseTime, unit));
  }

  @Override
  public RFuture<Boolean> tryLockAsync(long waitTime, long leaseTime,
      TimeUnit unit, long threadId) {
    Span span = tracingHelper.buildSpan("tryLockAsync", lock);
    span.setTag("waitTime", waitTime);
    span.setTag("leaseTime", leaseTime);
    span.setTag("unit", nullable(unit));
    span.setTag("threadId", threadId);
    return tracingHelper
        .prepareRFuture(span, () -> lock.tryLockAsync(waitTime, leaseTime, unit, threadId));
  }

  @Override
  public RFuture<Integer> getHoldCountAsync() {
    Span span = tracingHelper.buildSpan("getHoldCountAsync", lock);
    return tracingHelper.prepareRFuture(span, lock::getHoldCountAsync);
  }
}
