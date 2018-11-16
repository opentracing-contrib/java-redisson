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
import java.util.Map.Entry;
import java.util.Set;
import org.redisson.api.RSet;
import org.redisson.api.RSetMultimap;

public class TracingRSetMultimap<K, V> extends TracingRMultimap<K, V> implements
    RSetMultimap<K, V> {
  private final RSetMultimap<K, V> set;
  private final TracingHelper tracingHelper;

  public TracingRSetMultimap(RSetMultimap<K, V> set, TracingHelper tracingHelper) {
    super(set, tracingHelper);
    this.set = set;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public RSet<V> get(K key) {
    Span span = tracingHelper.buildSpan("get", set);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> new TracingRSet<>(set.get(key), tracingHelper));
  }

  @Override
  public Set<V> getAll(K key) {
    Span span = tracingHelper.buildSpan("getAll", set);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> set.getAll(key));
  }

  @Override
  public Set<V> removeAll(Object key) {
    Span span = tracingHelper.buildSpan("removeAll", set);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> set.removeAll(key));
  }

  @Override
  public Set<V> replaceValues(K key, Iterable<? extends V> values) {
    Span span = tracingHelper.buildSpan("replaceValues", set);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingHelper.decorate(span, () -> set.replaceValues(key, values));
  }

  @Override
  public Set<Entry<K, V>> entries() {
    Span span = tracingHelper.buildSpan("entries", set);
    return tracingHelper.decorate(span, set::entries);
  }

}
