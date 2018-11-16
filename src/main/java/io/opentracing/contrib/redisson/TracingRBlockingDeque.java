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
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RFuture;

public class TracingRBlockingDeque<V> extends TracingRDeque<V> implements RBlockingDeque<V> {
  private final RBlockingDeque<V> deque;
  private final TracingHelper tracingHelper;

  public TracingRBlockingDeque(RBlockingDeque<V> deque, TracingHelper tracingHelper) {
    super(deque, tracingHelper);
    this.deque = deque;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public V pollFirstFromAny(long timeout, TimeUnit unit, String... queueNames)
      throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollFirstFromAny", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .decorateThrowing(span, () -> deque.pollFirstFromAny(timeout, unit, queueNames));
  }

  @Override
  public V pollLastFromAny(long timeout, TimeUnit unit, String... queueNames)
      throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollLastFromAny", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .decorateThrowing(span, () -> deque.pollLastFromAny(timeout, unit, queueNames));
  }

  @Override
  public void addFirst(V v) {
    Span span = tracingHelper.buildSpan("addFirst", deque);
    span.setTag("element", nullable(v));
    tracingHelper.decorate(span, () -> deque.addFirst(v));
  }

  @Override
  public void addLast(V v) {
    Span span = tracingHelper.buildSpan("addLast", deque);
    span.setTag("element", nullable(v));
    tracingHelper.decorate(span, () -> deque.addLast(v));
  }

  @Override
  public boolean offerFirst(V v) {
    Span span = tracingHelper.buildSpan("offerFirst", deque);
    span.setTag("element", nullable(v));
    return tracingHelper.decorate(span, () -> deque.offerFirst(v));
  }

  @Override
  public boolean offerLast(V v) {
    Span span = tracingHelper.buildSpan("offerLast", deque);
    span.setTag("element", nullable(v));
    return tracingHelper.decorate(span, () -> deque.offerLast(v));
  }

  @Override
  public void putFirst(V v) throws InterruptedException {
    Span span = tracingHelper.buildSpan("putFirst", deque);
    span.setTag("element", nullable(v));
    tracingHelper.decorateThrowing(span, () -> deque.putFirst(v));
  }

  @Override
  public void putLast(V v) throws InterruptedException {
    Span span = tracingHelper.buildSpan("putLast", deque);
    span.setTag("element", nullable(v));
    tracingHelper.decorateThrowing(span, () -> deque.putLast(v));
  }

  @Override
  public boolean offerFirst(V v, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("offerFirst", deque);
    span.setTag("element", nullable(v));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> deque.offerFirst(v, timeout, unit));
  }

  @Override
  public boolean offerLast(V v, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("offerLast", deque);
    span.setTag("element", nullable(v));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> deque.offerLast(v, timeout, unit));
  }

  @Override
  public V takeFirst() throws InterruptedException {
    Span span = tracingHelper.buildSpan("takeFirst", deque);
    return tracingHelper.decorateThrowing(span, deque::takeFirst);
  }

  @Override
  public V takeLast() throws InterruptedException {
    Span span = tracingHelper.buildSpan("takeLast", deque);
    return tracingHelper.decorateThrowing(span, deque::takeLast);
  }

  @Override
  public V pollFirst(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollFirst", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> deque.pollFirst(timeout, unit));
  }

  @Override
  public V pollLast(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollLast", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> deque.pollLast(timeout, unit));
  }

  @Override
  public boolean removeFirstOccurrence(Object o) {
    Span span = tracingHelper.buildSpan("removeFirstOccurrence", deque);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> deque.removeFirstOccurrence(o));
  }

  @Override
  public boolean removeLastOccurrence(Object o) {
    Span span = tracingHelper.buildSpan("removeLastOccurrence", deque);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> deque.removeLastOccurrence(o));
  }

  @Override
  public boolean add(V v) {
    Span span = tracingHelper.buildSpan("add", deque);
    span.setTag("element", nullable(v));
    return tracingHelper.decorate(span, () -> deque.add(v));
  }

  @Override
  public boolean offer(V v) {
    Span span = tracingHelper.buildSpan("offer", deque);
    span.setTag("element", nullable(v));
    return tracingHelper.decorate(span, () -> deque.offer(v));
  }

  @Override
  public void put(V v) throws InterruptedException {
    Span span = tracingHelper.buildSpan("put", deque);
    span.setTag("element", nullable(v));
    tracingHelper.decorateThrowing(span, () -> deque.put(v));
  }

  @Override
  public boolean offer(V v, long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("offer", deque);
    span.setTag("element", nullable(v));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> deque.offer(v, timeout, unit));
  }

  @Override
  public V remove() {
    Span span = tracingHelper.buildSpan("remove", deque);
    return tracingHelper.decorate(span, () -> deque.remove());
  }

  @Override
  public V poll() {
    Span span = tracingHelper.buildSpan("poll", deque);
    return tracingHelper.decorate(span, () -> deque.poll());
  }

  @Override
  public V take() throws InterruptedException {
    Span span = tracingHelper.buildSpan("take", deque);
    return tracingHelper.decorateThrowing(span, deque::take);
  }

  @Override
  public V poll(long timeout, TimeUnit unit) throws InterruptedException {
    Span span = tracingHelper.buildSpan("poll", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorateThrowing(span, () -> deque.poll(timeout, unit));
  }

  @Override
  public V element() {
    Span span = tracingHelper.buildSpan("element", deque);
    return tracingHelper.decorate(span, deque::element);
  }

  @Override
  public V peek() {
    Span span = tracingHelper.buildSpan("peek", deque);
    return tracingHelper.decorate(span, deque::peek);
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingHelper.buildSpan("remove", deque);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> deque.remove(o));
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingHelper.buildSpan("contains", deque);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> deque.contains(o));
  }

  @Override
  public int size() {
    Span span = tracingHelper.buildSpan("size", deque);
    return tracingHelper.decorate(span, deque::size);
  }

  @Override
  public Iterator<V> iterator() {
    return deque.iterator();
  }

  @Override
  public void push(V v) {
    Span span = tracingHelper.buildSpan("push", deque);
    span.setTag("element", nullable(v));
    tracingHelper.decorate(span, () -> deque.push(v));
  }

  @Override
  public int remainingCapacity() {
    Span span = tracingHelper.buildSpan("remainingCapacity", deque);
    return tracingHelper.decorate(span, deque::remainingCapacity);
  }

  @Override
  public int drainTo(Collection<? super V> c) {
    Span span = tracingHelper.buildSpan("drainTo", deque);
    return tracingHelper.decorate(span, () -> deque.drainTo(c));
  }

  @Override
  public int drainTo(Collection<? super V> c, int maxElements) {
    Span span = tracingHelper.buildSpan("drainTo", deque);
    span.setTag("maxElements", maxElements);
    return tracingHelper.decorate(span, () -> deque.drainTo(c, maxElements));
  }

  @Override
  public V pollFromAny(long timeout, TimeUnit unit, String... queueNames)
      throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollFromAny", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper.decorateThrowing(span, () -> deque.pollFromAny(timeout, unit, queueNames));
  }

  @Override
  public V pollLastAndOfferFirstTo(String queueName, long timeout, TimeUnit unit)
      throws InterruptedException {
    Span span = tracingHelper.buildSpan("pollLastAndOfferFirstTo", deque);
    span.setTag("queueName", nullable(queueName));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .decorateThrowing(span, () -> deque.pollLastAndOfferFirstTo(queueName, timeout, unit));
  }

  @Override
  public V takeLastAndOfferFirstTo(String queueName) throws InterruptedException {
    Span span = tracingHelper.buildSpan("takeLastAndOfferFirstTo", deque);
    span.setTag("queueName", nullable(queueName));
    return tracingHelper.decorateThrowing(span, () -> deque.takeLastAndOfferFirstTo(queueName));
  }

  @Override
  public RFuture<V> pollFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollFromAnyAsync", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .prepareRFuture(span, () -> deque.pollFromAnyAsync(timeout, unit, queueNames));
  }

  @Override
  public RFuture<Integer> drainToAsync(Collection<? super V> c, int maxElements) {
    Span span = tracingHelper.buildSpan("drainToAsync", deque);
    span.setTag("maxElements", maxElements);
    return tracingHelper.prepareRFuture(span, () -> deque.drainToAsync(c, maxElements));
  }

  @Override
  public RFuture<Integer> drainToAsync(Collection<? super V> c) {
    Span span = tracingHelper.buildSpan("drainToAsync", deque);
    return tracingHelper.prepareRFuture(span, () -> deque.drainToAsync(c));
  }

  @Override
  public RFuture<V> pollLastAndOfferFirstToAsync(String queueName, long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollLastAndOfferFirstToAsync", deque);
    span.setTag("queueName", nullable(queueName));
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .prepareRFuture(span, () -> deque.pollLastAndOfferFirstToAsync(queueName, timeout, unit));
  }

  @Override
  public RFuture<V> takeLastAndOfferFirstToAsync(String queueName) {
    Span span = tracingHelper.buildSpan("takeLastAndOfferFirstToAsync", deque);
    span.setTag("queueName", nullable(queueName));
    return tracingHelper
        .prepareRFuture(span, () -> deque.takeLastAndOfferFirstToAsync(queueName));
  }

  @Override
  public RFuture<V> pollAsync(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollAsync", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> deque.pollAsync(timeout, unit));
  }

  @Override
  public RFuture<V> takeAsync() {
    Span span = tracingHelper.buildSpan("takeAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::takeAsync);
  }

  @Override
  public RFuture<Void> putAsync(V e) {
    Span span = tracingHelper.buildSpan("putAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.putAsync(e));
  }

  @Override
  public RFuture<V> pollFirstFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollFirstFromAnyAsync", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .prepareRFuture(span, () -> deque.pollFirstFromAnyAsync(timeout, unit, queueNames));
  }

  @Override
  public RFuture<V> pollLastFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollLastFromAnyAsync", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .prepareRFuture(span, () -> deque.pollLastFromAnyAsync(timeout, unit, queueNames));
  }

  @Override
  public RFuture<Void> putFirstAsync(V e) {
    Span span = tracingHelper.buildSpan("putFirstAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.putFirstAsync(e));
  }

  @Override
  public RFuture<Void> putLastAsync(V e) {
    Span span = tracingHelper.buildSpan("putLastAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.putLastAsync(e));
  }

  @Override
  public RFuture<V> pollLastAsync(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollLastAsync", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .prepareRFuture(span, () -> deque.pollLastAsync(timeout, unit));
  }

  @Override
  public RFuture<V> takeLastAsync() {
    Span span = tracingHelper.buildSpan("takeLastAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::takeLastAsync);
  }

  @Override
  public RFuture<V> pollFirstAsync(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollFirstAsync", deque);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper
        .prepareRFuture(span, () -> deque.pollFirstAsync(timeout, unit));
  }

  @Override
  public RFuture<V> takeFirstAsync() {
    Span span = tracingHelper.buildSpan("takeFirstAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::takeFirstAsync);
  }

}
