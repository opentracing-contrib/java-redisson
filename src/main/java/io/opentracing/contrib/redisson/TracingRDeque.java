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
import java.util.Iterator;
import org.redisson.api.RDeque;
import org.redisson.api.RFuture;

public class TracingRDeque<V> extends TracingRQueue<V> implements RDeque<V> {
  private final RDeque<V> deque;
  private final TracingHelper tracingHelper;

  public TracingRDeque(RDeque<V> deque, TracingHelper tracingHelper) {
    super(deque, tracingHelper);
    this.deque = deque;
    this.tracingHelper = tracingHelper;
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
  public V removeFirst() {
    Span span = tracingHelper.buildSpan("removeFirst", deque);
    return tracingHelper.decorate(span, deque::removeFirst);
  }

  @Override
  public V removeLast() {
    Span span = tracingHelper.buildSpan("removeLast", deque);
    return tracingHelper.decorate(span, deque::removeLast);
  }

  @Override
  public V pollFirst() {
    Span span = tracingHelper.buildSpan("pollFirst", deque);
    return tracingHelper.decorate(span, deque::pollFirst);
  }

  @Override
  public V pollLast() {
    Span span = tracingHelper.buildSpan("pollLast", deque);
    return tracingHelper.decorate(span, deque::pollLast);
  }

  @Override
  public V getFirst() {
    Span span = tracingHelper.buildSpan("getFirst", deque);
    return tracingHelper.decorate(span, deque::getFirst);
  }

  @Override
  public V getLast() {
    Span span = tracingHelper.buildSpan("getLast", deque);
    return tracingHelper.decorate(span, deque::getLast);
  }

  @Override
  public V peekFirst() {
    Span span = tracingHelper.buildSpan("peekFirst", deque);
    return tracingHelper.decorate(span, deque::peekFirst);
  }

  @Override
  public V peekLast() {
    Span span = tracingHelper.buildSpan("peekLast", deque);
    return tracingHelper.decorate(span, deque::peekLast);
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
  public V remove() {
    Span span = tracingHelper.buildSpan("remove", deque);
    return tracingHelper.decorate(span, () -> deque.remove());
  }

  @Override
  public V poll() {
    Span span = tracingHelper.buildSpan("poll", deque);
    return tracingHelper.decorate(span, deque::poll);
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
  public void push(V v) {
    Span span = tracingHelper.buildSpan("push", deque);
    span.setTag("element", nullable(v));
    tracingHelper.decorate(span, () -> deque.push(v));
  }

  @Override
  public V pop() {
    Span span = tracingHelper.buildSpan("pop", deque);
    return tracingHelper.decorate(span, deque::pop);
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
  public Iterator<V> descendingIterator() {
    return deque.descendingIterator();
  }

  @Override
  public RFuture<Boolean> removeLastOccurrenceAsync(Object o) {
    Span span = tracingHelper.buildSpan("removeLastOccurrenceAsync", deque);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> deque.removeLastOccurrenceAsync(o));
  }

  @Override
  public RFuture<V> removeLastAsync() {
    Span span = tracingHelper.buildSpan("removeLastAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::removeLastAsync);
  }

  @Override
  public RFuture<V> removeFirstAsync() {
    Span span = tracingHelper.buildSpan("removeFirstAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::removeFirstAsync);
  }

  @Override
  public RFuture<Boolean> removeFirstOccurrenceAsync(Object o) {
    Span span = tracingHelper.buildSpan("removeFirstOccurrenceAsync", deque);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> deque.removeFirstOccurrenceAsync(o));
  }

  @Override
  public RFuture<Void> pushAsync(V e) {
    Span span = tracingHelper.buildSpan("pushAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.pushAsync(e));
  }

  @Override
  public RFuture<V> popAsync() {
    Span span = tracingHelper.buildSpan("popAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::popAsync);
  }

  @Override
  public RFuture<V> pollLastAsync() {
    Span span = tracingHelper.buildSpan("pollLastAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::pollLastAsync);
  }

  @Override
  public RFuture<V> pollFirstAsync() {
    Span span = tracingHelper.buildSpan("pollFirstAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::pollFirstAsync);
  }

  @Override
  public RFuture<V> peekLastAsync() {
    Span span = tracingHelper.buildSpan("peekLastAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::peekLastAsync);
  }

  @Override
  public RFuture<V> peekFirstAsync() {
    Span span = tracingHelper.buildSpan("peekFirstAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::peekFirstAsync);
  }

  @Override
  public RFuture<Boolean> offerLastAsync(V e) {
    Span span = tracingHelper.buildSpan("offerLastAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.offerLastAsync(e));
  }

  @Override
  public RFuture<V> getLastAsync() {
    Span span = tracingHelper.buildSpan("getLastAsync", deque);
    return tracingHelper.prepareRFuture(span, deque::getLastAsync);
  }

  @Override
  public RFuture<Void> addLastAsync(V e) {
    Span span = tracingHelper.buildSpan("addLastAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.addLastAsync(e));
  }

  @Override
  public RFuture<Void> addFirstAsync(V e) {
    Span span = tracingHelper.buildSpan("addFirstAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.addFirstAsync(e));
  }

  @Override
  public RFuture<Boolean> offerFirstAsync(V e) {
    Span span = tracingHelper.buildSpan("offerFirstAsync", deque);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> deque.offerFirstAsync(e));
  }

}
