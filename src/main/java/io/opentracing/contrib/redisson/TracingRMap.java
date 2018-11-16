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

import static io.opentracing.contrib.redisson.TracingHelper.collectionToString;
import static io.opentracing.contrib.redisson.TracingHelper.mapToString;
import static io.opentracing.contrib.redisson.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RReadWriteLock;
import org.redisson.api.mapreduce.RMapReduce;

public class TracingRMap<K, V> extends TracingRExpirable implements RMap<K, V> {
  private final RMap<K, V> map;
  private final TracingHelper tracingHelper;

  public TracingRMap(RMap<K, V> map, TracingHelper tracingHelper) {
    super(map, tracingHelper);
    this.map = map;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public void loadAll(boolean replaceExistingValues, int parallelism) {
    Span span = tracingHelper.buildSpan("loadAll", map);
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    tracingHelper.decorate(span, () -> map.loadAll(replaceExistingValues, parallelism));
  }

  @Override
  public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, int parallelism) {
    Span span = tracingHelper.buildSpan("loadAll", map);
    span.setTag("keys", collectionToString(keys));
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    tracingHelper.decorate(span, () -> map.loadAll(keys, replaceExistingValues, parallelism));
  }

  @Override
  public V get(Object key) {
    Span span = tracingHelper.buildSpan("get", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.get(key));
  }

  @Override
  public V put(K key, V value) {
    Span span = tracingHelper.buildSpan("put", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.put(key, value));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    Span span = tracingHelper.buildSpan("putIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.putIfAbsent(key, value));
  }

  @Override
  public <KOut, VOut> RMapReduce<K, V, KOut, VOut> mapReduce() {
    return new TracingRMapReduce<>(map.mapReduce(), tracingHelper);
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
  public int valueSize(K key) {
    Span span = tracingHelper.buildSpan("valueSize", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.valueSize(key));
  }

  @Override
  public V addAndGet(K key, Number delta) {
    Span span = tracingHelper.buildSpan("addAndGet", map);
    span.setTag("key", nullable(key));
    span.setTag("delta", nullable(delta));
    return tracingHelper.decorate(span, () -> map.addAndGet(key, delta));
  }

  @Override
  public V remove(Object key) {
    Span span = tracingHelper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.remove(key));
  }

  @Override
  public V replace(K key, V value) {
    Span span = tracingHelper.buildSpan("replace", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.replace(key, value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Span span = tracingHelper.buildSpan("replace", map);
    span.setTag("key", nullable(key));
    span.setTag("oldValue", nullable(oldValue));
    span.setTag("newValue", nullable(newValue));
    return tracingHelper.decorate(span, () -> map.replace(key, oldValue, newValue));
  }

  @Override
  public boolean remove(Object key, Object value) {
    Span span = tracingHelper.buildSpan("remove", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.remove(key, value));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    Span span = tracingHelper.buildSpan("putAll", this.map);
    span.setTag("map", mapToString(map));
    tracingHelper.decorate(span, () -> this.map.putAll(map));
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map, int batchSize) {
    Span span = tracingHelper.buildSpan("putAll", this.map);
    span.setTag("map", mapToString(map));
    span.setTag("batchSize", batchSize);
    tracingHelper.decorate(span, () -> this.map.putAll(map, batchSize));
  }

  @Override
  public Map<K, V> getAll(Set<K> keys) {
    Span span = tracingHelper.buildSpan("getAll", map);
    span.setTag("keys", collectionToString(keys));
    return tracingHelper.decorate(span, () -> map.getAll(keys));
  }

  @Override
  public long fastRemove(K... keys) {
    Span span = tracingHelper.buildSpan("fastRemove", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.decorate(span, () -> map.fastRemove(keys));
  }

  @Override
  public boolean fastPut(K key, V value) {
    Span span = tracingHelper.buildSpan("fastPut", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.fastPut(key, value));
  }

  @Override
  public boolean fastReplace(K key, V value) {
    Span span = tracingHelper.buildSpan("fastReplace", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.fastReplace(key, value));
  }

  @Override
  public boolean fastPutIfAbsent(K key, V value) {
    Span span = tracingHelper.buildSpan("fastPutIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> map.fastPutIfAbsent(key, value));
  }

  @Override
  public Set<K> readAllKeySet() {
    Span span = tracingHelper.buildSpan("readAllKeySet", map);
    return tracingHelper.decorate(span, map::readAllKeySet);
  }

  @Override
  public Collection<V> readAllValues() {
    Span span = tracingHelper.buildSpan("readAllValues", map);
    return tracingHelper.decorate(span, map::readAllValues);
  }

  @Override
  public Set<Entry<K, V>> readAllEntrySet() {
    Span span = tracingHelper.buildSpan("readAllEntrySet", map);
    return tracingHelper.decorate(span, map::readAllEntrySet);
  }

  @Override
  public Map<K, V> readAllMap() {
    Span span = tracingHelper.buildSpan("readAllMap", map);
    return tracingHelper.decorate(span, map::readAllMap);
  }

  @Override
  public Set<K> keySet() {
    Span span = tracingHelper.buildSpan("keySet", map);
    return tracingHelper.decorate(span, () -> map.keySet());
  }

  @Override
  public Set<K> keySet(int count) {
    Span span = tracingHelper.buildSpan("keySet", map);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> map.keySet(count));
  }

  @Override
  public Set<K> keySet(String pattern, int count) {
    Span span = tracingHelper.buildSpan("keySet", map);
    span.setTag("pattern", pattern);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> map.keySet(pattern, count));
  }

  @Override
  public Set<K> keySet(String pattern) {
    Span span = tracingHelper.buildSpan("keySet", map);
    span.setTag("pattern", pattern);
    return tracingHelper.decorate(span, () -> map.keySet(pattern));
  }

  @Override
  public Collection<V> values() {
    Span span = tracingHelper.buildSpan("values", map);
    return tracingHelper.decorate(span, () -> map.values());
  }

  @Override
  public Collection<V> values(String keyPattern) {
    Span span = tracingHelper.buildSpan("values", map);
    span.setTag("keyPattern", keyPattern);
    return tracingHelper.decorate(span, () -> map.values(keyPattern));
  }

  @Override
  public Collection<V> values(String keyPattern, int count) {
    Span span = tracingHelper.buildSpan("values", map);
    span.setTag("keyPattern", keyPattern);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> map.values(keyPattern, count));
  }

  @Override
  public Collection<V> values(int count) {
    Span span = tracingHelper.buildSpan("values", map);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> map.values(count));
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Span span = tracingHelper.buildSpan("entrySet", map);
    return tracingHelper.decorate(span, () -> map.entrySet());
  }

  @Override
  public Set<Entry<K, V>> entrySet(String keyPattern) {
    Span span = tracingHelper.buildSpan("entrySet", map);
    span.setTag("keyPattern", keyPattern);
    return tracingHelper.decorate(span, () -> map.entrySet(keyPattern));
  }

  @Override
  public Set<Entry<K, V>> entrySet(String keyPattern, int count) {
    Span span = tracingHelper.buildSpan("entrySet", map);
    span.setTag("keyPattern", keyPattern);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> map.entrySet(keyPattern, count));
  }

  @Override
  public Set<Entry<K, V>> entrySet(int count) {
    Span span = tracingHelper.buildSpan("entrySet", map);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> map.entrySet(count));
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    Span span = tracingHelper.buildSpan("getOrDefault", map);
    span.setTag("key", nullable(key));
    span.setTag("defaultValue", nullable(defaultValue));
    return tracingHelper.decorate(span, () -> map.getOrDefault(key, defaultValue));
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    Span span = tracingHelper.buildSpan("forEach", map);
    span.setTag("action", nullable(action));
    tracingHelper.decorate(span, () -> map.forEach(action));
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    Span span = tracingHelper.buildSpan("replaceAll", map);
    span.setTag("function", nullable(function));
    tracingHelper.decorate(span, () -> map.replaceAll(function));
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    Span span = tracingHelper.buildSpan("computeIfAbsent", map);
    span.setTag("key", nullable(key));
    span.setTag("mappingFunction", nullable(mappingFunction));
    return tracingHelper.decorate(span, () -> map.computeIfAbsent(key, mappingFunction));
  }

  @Override
  public V computeIfPresent(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    Span span = tracingHelper.buildSpan("computeIfPresent", map);
    span.setTag("key", nullable(key));
    span.setTag("remappingFunction", nullable(remappingFunction));
    return tracingHelper.decorate(span, () -> map.computeIfPresent(key, remappingFunction));
  }

  @Override
  public V compute(K key,
      BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    Span span = tracingHelper.buildSpan("compute", map);
    span.setTag("key", nullable(key));
    span.setTag("remappingFunction", nullable(remappingFunction));
    return tracingHelper.decorate(span, () -> map.compute(key, remappingFunction));
  }

  @Override
  public V merge(K key, V value,
      BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    Span span = tracingHelper.buildSpan("merge", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("remappingFunction", nullable(remappingFunction));
    return tracingHelper.decorate(span, () -> map.merge(key, value, remappingFunction));
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
  public void clear() {
    Span span = tracingHelper.buildSpan("clear", map);
    tracingHelper.decorate(span, map::clear);
  }

  @Override
  public boolean equals(Object o) {
    Span span = tracingHelper.buildSpan("equals", map);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> map.equals(o));
  }

  @Override
  public int hashCode() {
    Span span = tracingHelper.buildSpan("hashCode", map);
    return tracingHelper.decorate(span, map::hashCode);
  }

  @Override
  public RFuture<Void> loadAllAsync(boolean replaceExistingValues, int parallelism) {
    Span span = tracingHelper.buildSpan("loadAllAsync", map);
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    return tracingHelper
        .prepareRFuture(span, () -> map.loadAllAsync(replaceExistingValues, parallelism));
  }

  @Override
  public RFuture<Void> loadAllAsync(Set<? extends K> keys,
      boolean replaceExistingValues, int parallelism) {
    Span span = tracingHelper.buildSpan("loadAllAsync", map);
    span.setTag("replaceExistingValues", replaceExistingValues);
    span.setTag("parallelism", parallelism);
    span.setTag("keys", collectionToString(keys));
    return tracingHelper
        .prepareRFuture(span, () -> map.loadAllAsync(keys, replaceExistingValues, parallelism));
  }

  @Override
  public RFuture<Integer> valueSizeAsync(K key) {
    Span span = tracingHelper.buildSpan("valueSizeAsync", map);
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> map.valueSizeAsync(key));
  }

  @Override
  public RFuture<Map<K, V>> getAllAsync(Set<K> keys) {
    Span span = tracingHelper.buildSpan("getAllAsync", map);
    span.setTag("keys", collectionToString(keys));
    return tracingHelper.prepareRFuture(span, () -> map.getAllAsync(keys));
  }

  @Override
  public RFuture<Void> putAllAsync(Map<? extends K, ? extends V> map) {
    Span span = tracingHelper.buildSpan("putAllAsync", this.map);
    span.setTag("map", mapToString(map));
    return tracingHelper.prepareRFuture(span, () -> this.map.putAllAsync(map));
  }

  @Override
  public RFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, int batchSize) {
    Span span = tracingHelper.buildSpan("putAllAsync", this.map);
    span.setTag("map", mapToString(map));
    span.setTag("batchSize", batchSize);
    return tracingHelper.prepareRFuture(span, () -> this.map.putAllAsync(map, batchSize));
  }

  @Override
  public RFuture<V> addAndGetAsync(K key, Number value) {
    Span span = tracingHelper.buildSpan("addAndGetAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.addAndGetAsync(key, value));
  }

  @Override
  public RFuture<Boolean> containsValueAsync(Object value) {
    Span span = tracingHelper.buildSpan("containsValueAsync", map);
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.containsValueAsync(value));
  }

  @Override
  public RFuture<Boolean> containsKeyAsync(Object key) {
    Span span = tracingHelper.buildSpan("containsKeyAsync", map);
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> map.containsKeyAsync(key));
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingHelper.buildSpan("sizeAsync", map);
    return tracingHelper.prepareRFuture(span, map::sizeAsync);
  }

  @Override
  public RFuture<Long> fastRemoveAsync(K... keys) {
    Span span = tracingHelper.buildSpan("fastRemoveAsync", map);
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.prepareRFuture(span, () -> map.fastRemoveAsync(keys));
  }

  @Override
  public RFuture<Boolean> fastPutAsync(K key, V value) {
    Span span = tracingHelper.buildSpan("fastPutAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.fastPutAsync(key, value));
  }

  @Override
  public RFuture<Boolean> fastReplaceAsync(K key, V value) {
    Span span = tracingHelper.buildSpan("fastReplaceAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.fastReplaceAsync(key, value));
  }

  @Override
  public RFuture<Boolean> fastPutIfAbsentAsync(K key, V value) {
    Span span = tracingHelper.buildSpan("fastPutIfAbsentAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.fastPutIfAbsentAsync(key, value));
  }

  @Override
  public RFuture<Set<K>> readAllKeySetAsync() {
    Span span = tracingHelper.buildSpan("readAllKeySetAsync", map);
    return tracingHelper.prepareRFuture(span, map::readAllKeySetAsync);
  }

  @Override
  public RFuture<Collection<V>> readAllValuesAsync() {
    Span span = tracingHelper.buildSpan("readAllValuesAsync", map);
    return tracingHelper.prepareRFuture(span, map::readAllValuesAsync);
  }

  @Override
  public RFuture<Set<Entry<K, V>>> readAllEntrySetAsync() {
    Span span = tracingHelper.buildSpan("readAllEntrySetAsync", map);
    return tracingHelper.prepareRFuture(span, map::readAllEntrySetAsync);
  }

  @Override
  public RFuture<Map<K, V>> readAllMapAsync() {
    Span span = tracingHelper.buildSpan("readAllMapAsync", map);
    return tracingHelper.prepareRFuture(span, map::readAllMapAsync);
  }

  @Override
  public RFuture<V> getAsync(K key) {
    Span span = tracingHelper.buildSpan("getAsync", map);
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> map.getAsync(key));
  }

  @Override
  public RFuture<V> putAsync(K key, V value) {
    Span span = tracingHelper.buildSpan("putAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.putAsync(key, value));
  }

  @Override
  public RFuture<V> removeAsync(K key) {
    Span span = tracingHelper.buildSpan("removeAsync", map);
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> map.removeAsync(key));
  }

  @Override
  public RFuture<V> replaceAsync(K key, V value) {
    Span span = tracingHelper.buildSpan("replaceAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.replaceAsync(key, value));
  }

  @Override
  public RFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
    Span span = tracingHelper.buildSpan("replaceAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("oldValue", nullable(oldValue));
    span.setTag("newValue", nullable(newValue));
    return tracingHelper.prepareRFuture(span, () -> map.replaceAsync(key, oldValue, newValue));
  }

  @Override
  public RFuture<Boolean> removeAsync(Object key, Object value) {
    Span span = tracingHelper.buildSpan("removeAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.removeAsync(key, value));
  }

  @Override
  public RFuture<V> putIfAbsentAsync(K key, V value) {
    Span span = tracingHelper.buildSpan("putIfAbsentAsync", map);
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> map.putIfAbsentAsync(key, value));
  }
}
