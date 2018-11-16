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
import org.redisson.api.RBucket;
import org.redisson.api.RFuture;

public class TracingRBucket<V> extends TracingRExpirable implements RBucket<V> {
  private final RBucket<V> bucket;
  private final TracingHelper tracingHelper;

  public TracingRBucket(RBucket<V> bucket, TracingHelper tracingHelper) {
    super(bucket, tracingHelper);
    this.bucket = bucket;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public long size() {
    Span span = tracingHelper.buildSpan("size", bucket);
    return tracingHelper.decorate(span, bucket::size);
  }

  @Override
  public V get() {
    Span span = tracingHelper.buildSpan("get", bucket);
    return tracingHelper.decorate(span, bucket::get);
  }

  @Override
  public V getAndDelete() {
    Span span = tracingHelper.buildSpan("getAndDelete", bucket);
    return tracingHelper.decorate(span, bucket::getAndDelete);
  }

  @Override
  public boolean trySet(V value) {
    Span span = tracingHelper.buildSpan("trySet", bucket);
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> bucket.trySet(value));
  }

  @Override
  public boolean trySet(V value, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("trySet", bucket);
    span.setTag("value", nullable(value));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper.decorate(span, () -> bucket.trySet(value, timeToLive, timeUnit));
  }

  @Override
  public boolean compareAndSet(V expect, V update) {
    Span span = tracingHelper.buildSpan("compareAndSet", bucket);
    span.setTag("expect", nullable(expect));
    span.setTag("update", nullable(update));
    return tracingHelper.decorate(span, () -> bucket.compareAndSet(expect, update));
  }

  @Override
  public V getAndSet(V newValue) {
    Span span = tracingHelper.buildSpan("getAndSet", bucket);
    span.setTag("newValue", nullable(newValue));
    return tracingHelper.decorate(span, () -> bucket.getAndSet(newValue));
  }

  @Override
  public void set(V value) {
    Span span = tracingHelper.buildSpan("set", bucket);
    span.setTag("value", nullable(value));
    tracingHelper.decorate(span, () -> bucket.set(value));
  }

  @Override
  public void set(V value, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("set", bucket);
    span.setTag("value", nullable(value));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    tracingHelper.decorate(span, () -> bucket.set(value, timeToLive, timeUnit));
  }

  @Override
  public RFuture<Long> sizeAsync() {
    Span span = tracingHelper.buildSpan("sizeAsync", bucket);
    return tracingHelper.prepareRFuture(span, bucket::sizeAsync);
  }

  @Override
  public RFuture<V> getAsync() {
    Span span = tracingHelper.buildSpan("getAsync", bucket);
    return tracingHelper.prepareRFuture(span, bucket::getAsync);
  }

  @Override
  public RFuture<V> getAndDeleteAsync() {
    Span span = tracingHelper.buildSpan("getAndDeleteAsync", bucket);
    return tracingHelper.prepareRFuture(span, bucket::getAndDeleteAsync);
  }

  @Override
  public RFuture<Boolean> trySetAsync(V value) {
    Span span = tracingHelper.buildSpan("trySetAsync", bucket);
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> bucket.trySetAsync(value));
  }

  @Override
  public RFuture<Boolean> trySetAsync(V value, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("trySetAsync", bucket);
    span.setTag("value", nullable(value));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper
        .prepareRFuture(span, () -> bucket.trySetAsync(value, timeToLive, timeUnit));
  }

  @Override
  public RFuture<Boolean> compareAndSetAsync(V expect, V update) {
    Span span = tracingHelper.buildSpan("compareAndSetAsync", bucket);
    span.setTag("expect", nullable(expect));
    span.setTag("update", nullable(update));
    return tracingHelper.prepareRFuture(span, () -> bucket.compareAndSetAsync(expect, update));
  }

  @Override
  public RFuture<V> getAndSetAsync(V newValue) {
    Span span = tracingHelper.buildSpan("getAndSetAsync", bucket);
    span.setTag("newValue", nullable(newValue));
    return tracingHelper.prepareRFuture(span, () -> bucket.getAndSetAsync(newValue));
  }

  @Override
  public RFuture<Void> setAsync(V value) {
    Span span = tracingHelper.buildSpan("setAsync", bucket);
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> bucket.setAsync(value));
  }

  @Override
  public RFuture<Void> setAsync(V value, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("setAsync", bucket);
    span.setTag("value", nullable(value));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper.prepareRFuture(span, () -> bucket.setAsync(value, timeToLive, timeUnit));
  }

}
