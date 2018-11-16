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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.redisson.api.RPriorityQueue;

public class TracingRPriorityQueue<V> extends TracingRObject implements RPriorityQueue<V> {
  private final RPriorityQueue<V> queue;
  private final TracingHelper tracingHelper;

  public TracingRPriorityQueue(RPriorityQueue<V> queue, TracingHelper tracingHelper) {
    super(queue, tracingHelper);
    this.queue = queue;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public Comparator<? super V> comparator() {
    return queue.comparator();
  }

  @Override
  public List<V> readAll() {
    Span span = tracingHelper.buildSpan("readAll", queue);
    return tracingHelper.decorate(span, queue::readAll);
  }

  @Override
  public V pollLastAndOfferFirstTo(String dequeName) {
    Span span = tracingHelper.buildSpan("pollLastAndOfferFirstTo", queue);
    span.setTag("dequeName", nullable(dequeName));
    return tracingHelper.decorate(span, () -> queue.pollLastAndOfferFirstTo(dequeName));
  }

  @Override
  public boolean trySetComparator(Comparator<? super V> comparator) {
    Span span = tracingHelper.buildSpan("trySetComparator", queue);
    span.setTag("comparator", nullable(comparator));
    return tracingHelper.decorate(span, () -> queue.trySetComparator(comparator));
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
  public V remove() {
    Span span = tracingHelper.buildSpan("remove", queue);
    return tracingHelper.decorate(span, () -> queue.remove());
  }

  @Override
  public V poll() {
    Span span = tracingHelper.buildSpan("poll", queue);
    return tracingHelper.decorate(span, queue::poll);
  }

  @Override
  public V element() {
    Span span = tracingHelper.buildSpan("element", queue);
    return tracingHelper.decorate(span, queue::element);
  }

  @Override
  public V peek() {
    Span span = tracingHelper.buildSpan("peek", queue);
    return tracingHelper.decorate(span, queue::peek);
  }

  @Override
  public int size() {
    Span span = tracingHelper.buildSpan("size", queue);
    return tracingHelper.decorate(span, queue::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingHelper.buildSpan("isEmpty", queue);
    return tracingHelper.decorate(span, queue::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingHelper.buildSpan("contains", queue);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> queue.contains(o));
  }

  @Override
  public Iterator<V> iterator() {
    return queue.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = tracingHelper.buildSpan("toArray", queue);
    return tracingHelper.decorate(span, () -> queue.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingHelper.buildSpan("toArray", queue);
    return tracingHelper.decorate(span, () -> queue.toArray(a));
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingHelper.buildSpan("remove", queue);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> queue.remove(o));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("containsAll", queue);
    return tracingHelper.decorate(span, () -> queue.containsAll(c));
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    Span span = tracingHelper.buildSpan("addAll", queue);
    return tracingHelper.decorate(span, () -> queue.addAll(c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("removeAll", queue);
    return tracingHelper.decorate(span, () -> queue.removeAll(c));
  }

  @Override
  public boolean removeIf(Predicate<? super V> filter) {
    Span span = tracingHelper.buildSpan("removeIf", queue);
    span.setTag("filter", nullable(filter));
    return tracingHelper.decorate(span, () -> queue.removeIf(filter));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("retainAll", queue);
    return tracingHelper.decorate(span, () -> queue.retainAll(c));
  }

  @Override
  public void clear() {
    Span span = tracingHelper.buildSpan("clear", queue);
    tracingHelper.decorate(span, queue::clear);
  }

  @Override
  public Spliterator<V> spliterator() {
    return queue.spliterator();
  }

  @Override
  public Stream<V> stream() {
    return queue.stream();
  }

  @Override
  public Stream<V> parallelStream() {
    return queue.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingHelper.buildSpan("forEach", queue);
    span.setTag("action", nullable(action));
    tracingHelper.decorate(span, () -> queue.forEach(action));
  }

}
