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
import java.util.Map.Entry;
import java.util.Set;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RMultimap;
import org.redisson.api.RReadWriteLock;

public class TracingRMultimap<K, V> extends TracingRExpirable implements RMultimap<K, V> {
  private final RMultimap<K, V> map;
  private final TracingHelper tracingHelper;

  public TracingRMultimap(RMultimap<K, V> map, TracingHelper tracingHelper) {
    super(map, tracingHelper);
    this.map = map;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public RReadWriteLock getReadWriteLock(K key) {
    return new TracingRReadWriteLock(map.getReadWriteLock(key), tracingHelper);
  }

  @Override
  public RLock getLock(K key) {
    return new TracingRLock(map.getLock(key), tracingHelper);
  }

  @Override
  public int size() {
    Span span = tracingHelper.buildSpan("size", map);
    return tracingHelper.decorate(span, map::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingHelper.buildSpan("isEmpty", map);
    return tracingHelper.decorate(span, map::isEmpty);
  }

  @Override
  public boolean containsKey(Object key) {
    Span span = tracingHelper.buildSpan("containsKey", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.containsKey(key));
  }

  @Override
  public boolean containsValue(Object value) {
    Span span = tracingHelper.buildSpan("containsValue", map);
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.containsValue(value));
  }

  @Override
  public boolean containsEntry(Object key, Object value) {
    Span span = tracingHelper.buildSpan("containsEntry", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.containsEntry(key, value));
  }

  @Override
  public boolean put(K key, V value) {
    Span span = tracingHelper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.put(key, value));
  }

  @Override
  public boolean remove(Object key, Object value) {
    Span span = tracingHelper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.remove(key, value));
  }

  @Override
  public boolean putAll(K key, Iterable<? extends V> values) {
    Span span = tracingHelper.buildSpan("putAll", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingHelper.decorate(span, () -> map.putAll(key, values));
  }

  @Override
  public Collection<V> replaceValues(K key, Iterable<? extends V> values) {
    Span span = tracingHelper.buildSpan("replaceValues", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingHelper.decorate(span, () -> map.replaceValues(key, values));
  }

  @Override
  public Collection<V> removeAll(Object key) {
    Span span = tracingHelper.buildSpan("removeAll", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.removeAll(key));
  }

  @Override
  public void clear() {
    Span span = tracingHelper.buildSpan("clear", map);
    tracingHelper.decorate(span, map::clear);
  }

  @Override
  public Collection<V> get(K key) {
    Span span = tracingHelper.buildSpan("get", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.get(key));
  }

  @Override
  public Collection<V> getAll(K key) {
    Span span = tracingHelper.buildSpan("getAll", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.getAll(key));
  }

  @Override
  public Set<K> keySet() {
    Span span = tracingHelper.buildSpan("keySet", map);
    return tracingHelper.decorate(span, map::keySet);
  }

  @Override
  public int keySize() {
    Span span = tracingHelper.buildSpan("keySize", map);
    return tracingHelper.decorate(span, map::keySize);
  }

  @Override
  public Collection<V> values() {
    Span span = tracingHelper.buildSpan("values", map);
    return tracingHelper.decorate(span, map::values);
  }

  @Override
  public Collection<Entry<K, V>> entries() {
    Span span = tracingHelper.buildSpan("entries", map);
    return tracingHelper.decorate(span, map::entries);
  }

  @Override
  public long fastRemove(K... keys) {
    Span span = tracingHelper.buildSpan("fastRemove", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.decorate(span, () -> map.fastRemove(keys));
  }

  @Override
  public Set<K> readAllKeySet() {
    Span span = tracingHelper.buildSpan("readAllKeySet", map);
    return tracingHelper.decorate(span, map::readAllKeySet);
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingHelper.buildSpan("sizeAsync", map);
    return tracingHelper.prepareRFuture(span, map::sizeAsync);
  }

  @Override
  public RFuture<Boolean> containsKeyAsync(Object key) {
    Span span = tracingHelper.buildSpan("containsKeyAsync", map);
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> map.containsKeyAsync(key));
  }

  @Override
  public RFuture<Boolean> containsValueAsync(Object value) {
    Span span = tracingHelper.buildSpan("containsValueAsync", map);
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.containsValueAsync(value));
  }

  @Override
  public RFuture<Boolean> containsEntryAsync(Object key, Object value) {
    Span span = tracingHelper.buildSpan("containsEntryAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.containsEntryAsync(key, value));
  }

  @Override
  public RFuture<Boolean> putAsync(K key, V value) {
    Span span = tracingHelper.buildSpan("putAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.putAsync(key, value));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object key, Object value) {
    Span span = tracingHelper.buildSpan("removeAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.removeAsync(key, value));
  }

  @Override
  public RFuture<Boolean> putAllAsync(K key, Iterable<? extends V> values) {
    Span span = tracingHelper.buildSpan("putAllAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingHelper.prepareRFuture(span, () -> map.putAllAsync(key, values));
  }

  @Override
  public RFuture<Collection<V>> replaceValuesAsync(K key, Iterable<? extends V> values) {
    Span span = tracingHelper.buildSpan("replaceValuesAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingHelper.prepareRFuture(span, () -> map.replaceValuesAsync(key, values));
  }

  @Override
  public RFuture<Collection<V>> removeAllAsync(Object key) {
    Span span = tracingHelper.buildSpan("removeAllAsync", map);
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> map.removeAllAsync(key));
  }

  @Override
  public RFuture<Collection<V>> getAllAsync(K key) {
    Span span = tracingHelper.buildSpan("getAllAsync", map);
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> map.getAllAsync(key));
  }

  @Override
  public RFuture<Integer> keySizeAsync() {
    Span span = tracingHelper.buildSpan("keySizeAsync", map);
    return tracingHelper.prepareRFuture(span, map::keySizeAsync);
  }

  @Override
  public RFuture<Long> fastRemoveAsync(K... keys) {
    Span span = tracingHelper.buildSpan("fastRemoveAsync", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.prepareRFuture(span, () -> map.fastRemoveAsync(keys));
  }

  @Override
  public RFuture<Set<K>> readAllKeySetAsync() {
    Span span = tracingHelper.buildSpan("readAllKeySetAsync", map);
    return tracingHelper.prepareRFuture(span, map::readAllKeySetAsync);
  }

}
