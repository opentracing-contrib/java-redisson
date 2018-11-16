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
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RSetCache;
import org.redisson.api.mapreduce.RCollectionMapReduce;

public class TracingRSetCache<V> extends TracingRExpirable implements RSetCache<V> {
  private final RSetCache<V> cache;
  private final TracingHelper tracingHelper;

  public TracingRSetCache(RSetCache<V> cache, TracingHelper tracingHelper) {
    super(cache, tracingHelper);
    this.cache = cache;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public RLock getLock(V value) {
    return new TracingRLock(cache.getLock(value), tracingHelper);
  }

  @Override
  public Iterator<V> iterator(int count) {
    return cache.iterator(count);
  }

  @Override
  public Iterator<V> iterator(String pattern, int count) {
    return cache.iterator(pattern, count);
  }

  @Override
  public Iterator<V> iterator(String pattern) {
    return cache.iterator(pattern);
  }

  @Override
  public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
    return new TracingRCollectionMapReduce<>(cache.mapReduce(), tracingHelper);
  }

  @Override
  public boolean add(V value, long ttl, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("add", cache);
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorate(span, () -> cache.add(value, ttl, unit));
  }

  @Override
  public int size() {
    Span span = tracingHelper.buildSpan("size", cache);
    return tracingHelper.decorate(span, cache::size);
  }

  @Override
  public Set<V> readAll() {
    Span span = tracingHelper.buildSpan("readAll", cache);
    return tracingHelper.decorate(span, cache::readAll);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingHelper.buildSpan("isEmpty", cache);
    return tracingHelper.decorate(span, cache::isEmpty);
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingHelper.buildSpan("contains", cache);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> cache.contains(o));
  }

  @Override
  public Iterator<V> iterator() {
    return cache.iterator();
  }

  @Override
  public Object[] toArray() {
    Span span = tracingHelper.buildSpan("toArray", cache);
    return tracingHelper.decorate(span, () -> cache.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingHelper.buildSpan("toArray", cache);
    return tracingHelper.decorate(span, () -> cache.toArray(a));
  }

  @Override
  public boolean add(V element) {
    Span span = tracingHelper.buildSpan("add", cache);
    span.setTag("element", nullable(element));
    return tracingHelper.decorate(span, () -> cache.add(element));
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingHelper.buildSpan("remove", cache);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> cache.remove(o));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("containsAll", cache);
    return tracingHelper.decorate(span, () -> cache.containsAll(c));
  }

  @Override
  public boolean addAll(Collection<? extends V> c) {
    Span span = tracingHelper.buildSpan("addAll", cache);
    return tracingHelper.decorate(span, () -> cache.addAll(c));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("retainAll", cache);
    return tracingHelper.decorate(span, () -> cache.retainAll(c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("removeAll", cache);
    return tracingHelper.decorate(span, () -> cache.removeAll(c));
  }

  @Override
  public void clear() {
    Span span = tracingHelper.buildSpan("clear", cache);
    tracingHelper.decorate(span, cache::clear);
  }

  @Override
  public Spliterator<V> spliterator() {
    return cache.spliterator();
  }

  @Override
  public boolean removeIf(Predicate<? super V> filter) {
    Span span = tracingHelper.buildSpan("removeIf", cache);
    span.setTag("filter", nullable(filter));
    return tracingHelper.decorate(span, () -> cache.removeIf(filter));
  }

  @Override
  public Stream<V> stream() {
    return cache.stream();
  }

  @Override
  public Stream<V> parallelStream() {
    return cache.parallelStream();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingHelper.buildSpan("forEach", cache);
    span.setTag("action", nullable(action));
    tracingHelper.decorate(span, () -> cache.forEach(action));
  }

  @Override
  public RFuture<Boolean> addAsync(V value, long ttl, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("addAsync", cache);
    span.setTag("value", nullable(value));
    span.setTag("ttl", ttl);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> cache.addAsync(value, ttl, unit));
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingHelper.buildSpan("sizeAsync", cache);
    return tracingHelper.prepareRFuture(span, cache::sizeAsync);
  }

  @Override
  public RFuture<Set<V>> readAllAsync() {
    Span span = tracingHelper.buildSpan("readAllAsync", cache);
    return tracingHelper.prepareRFuture(span, cache::readAllAsync);
  }

  @Override
  public RFuture<Boolean> retainAllAsync(Collection<?> c) {
    Span span = tracingHelper.buildSpan("retainAllAsync", cache);
    return tracingHelper.prepareRFuture(span, () -> cache.retainAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAllAsync(Collection<?> c) {
    Span span = tracingHelper.buildSpan("removeAllAsync", cache);
    return tracingHelper.prepareRFuture(span, () -> cache.removeAllAsync(c));
  }

  @Override
  public RFuture<Boolean> containsAsync(Object o) {
    Span span = tracingHelper.buildSpan("containsAsync", cache);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> cache.containsAsync(o));
  }

  @Override
  public RFuture<Boolean> containsAllAsync(Collection<?> c) {
    Span span = tracingHelper.buildSpan("containsAllAsync", cache);
    return tracingHelper.prepareRFuture(span, () -> cache.containsAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object o) {
    Span span = tracingHelper.buildSpan("removeAsync", cache);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> cache.removeAsync(o));
  }

  @Override
  public RFuture<Boolean> addAsync(V e) {
    Span span = tracingHelper.buildSpan("addAsync", cache);
    span.setTag("element", nullable(e));
    return tracingHelper.prepareRFuture(span, () -> cache.addAsync(e));
  }

  @Override
  public RFuture<Boolean> addAllAsync(Collection<? extends V> c) {
    Span span = tracingHelper.buildSpan("addAllAsync", cache);
    return tracingHelper.prepareRFuture(span, () -> cache.addAllAsync(c));
  }

  @Override
  public void destroy() {
    Span span = tracingHelper.buildSpan("destroy", cache);
    tracingHelper.decorate(span, cache::destroy);
  }


}
