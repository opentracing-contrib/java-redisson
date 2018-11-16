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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RFuture;

public class TracingRBlockingQueue<V> extends TracingRQueue<V> implements RBlockingQueue<V> {
  private final RBlockingQueue<V> queue;
  private final TracingHelper tracingHelper;

  public TracingRBlockingQueue(RBlockingQueue<V> queue, TracingHelper tracingHelper) {
    super(queue, tracingHelper);
    this.queue = queue;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public V pollFromAny(long timeout, TimeUnit unit, String... queueNames)
      throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollFromAny", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper.decorateThrowing(span, () -> queue.pollFromAny(timeout, unit, queueNames));
  }

  @Override
  public V pollLastAndOfferFirstTo(String queueName, long timeout, TimeUnit unit)
      throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollLastAndOfferFirstTo", queue);
    span.setTag("queueName", nullable(queueName));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .decorateThrowing(span, () -> queue.pollLastAndOfferFirstTo(queueName, timeout, unit));
  }

  @Override
  public V takeLastAndOfferFirstTo(String queueName) throws InterruptedException {
    Span span = tracingHelper.buildSpan("takeLastAndOfferFirstTo", queue);
    span.setTag("queueName", nullable(queueName));
    return tracingHelper.decorateThrowing(span, () -> queue.takeLastAndOfferFirstTo(queueName));
  }

  @Override
  public boolean add(V v) {
    Span span = tracingHelper.buildSpan("add", queue);
    span.setTag("element", nullable(v));
    return tracingHelper.decorate(span, () -> queue.add(v));
  }

  @Override
  public boolean offer(V v) {
    Span span = tracingHelper.buildSpan("offer", queue);
    span.setTag("element", nullable(v));
    return tracingHelper.decorate(span, () -> queue.offer(v));
  }

  @Override
  public void put(V v) throws InterruptedException {
    Span span = tracingHelper.buildSpan("put", queue);
    span.setTag("element", nullable(v));
    tracingHelper.decorateThrowing(span, () -> queue.put(v));
  }

  @Override
  public boolean offer(V v, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("offer", queue);
    span.setTag("element", nullable(v));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> queue.offer(v, timeout, unit));
  }

  @Override
  public V take() throws InterruptedException {
    Span span = tracingHelper.buildSpan("take", queue);
    return tracingHelper.decorateThrowing(span, queue::take);
  }

  @Override
  public V poll(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("poll", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> queue.poll(timeout, unit));
  }

  @Override
  public int remainingCapacity() {
    Span span = tracingHelper.buildSpan("remainingCapacity", queue);
    return tracingHelper.decorate(span, queue::remainingCapacity);
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingHelper.buildSpan("remove", queue);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> queue.remove(o));
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingHelper.buildSpan("contains", queue);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> queue.contains(o));
  }

  @Override
  public int drainTo(Collection<? super V> c) {
    Span span = tracingHelper.buildSpan("drainTo", queue);
    return tracingHelper.decorate(span, () -> queue.drainTo(c));
  }

  @Override
  public int drainTo(Collection<? super V> c, int maxElements) {
    Span span = tracingHelper.buildSpan("drainTo", queue);
    span.setTag("maxElements", maxElements);
    return tracingHelper.decorate(span, () -> queue.drainTo(c, maxElements));
  }

  @Override
  public RFuture<V> pollFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollFromAnyAsync", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .prepareRFuture(span, () -> queue.pollFromAnyAsync(timeout, unit, queueNames));
  }

  @Override
  public RFuture<Integer> drainToAsync(Collection<? super V> c, int maxElements) {
    Span span = tracingHelper.buildSpan("drainToAsync", queue);
    span.setTag("maxElements", maxElements);
    return tracingHelper.prepareRFuture(span, () -> queue.drainToAsync(c, maxElements));
  }

  @Override
  public RFuture<Integer> drainToAsync(Collection<? super V> c) {
    Span span = tracingHelper.buildSpan("drainToAsync", queue);
    return tracingHelper.prepareRFuture(span, () -> queue.drainToAsync(c));
  }

  @Override
  public RFuture<V> pollLastAndOfferFirstToAsync(String queueName, long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollLastAndOfferFirstToAsync", queue);
    span.setTag("queueName", nullable(queueName));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .prepareRFuture(span, () -> queue.pollLastAndOfferFirstToAsync(queueName, timeout, unit));
  }

  @Override
  public RFuture<V> takeLastAndOfferFirstToAsync(String queueName) {
    Span span = tracingHelper.buildSpan("takeLastAndOfferFirstToAsync", queue);
    span.setTag("queueName", nullable(queueName));
    return tracingHelper
        .prepareRFuture(span, () -> queue.takeLastAndOfferFirstToAsync(queueName));
  }

  @Override
  public RFuture<V> pollAsync(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollAsync", queue);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> queue.pollAsync(timeout, unit));
  }

  @Override
  public RFuture<V> takeAsync() {
    Span span = tracingHelper.buildSpan("takeAsync", queue);
    return tracingHelper.prepareRFuture(span, queue::takeAsync);
  }

  @Override
  public RFuture<Void> putAsync(V e) {
    Span span = tracingHelper.buildSpan("putAsync", queue);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> queue.putAsync(e));
  }

}
