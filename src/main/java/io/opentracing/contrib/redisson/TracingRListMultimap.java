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
import java.util.List;
import org.redisson.api.RList;
import org.redisson.api.RListMultimap;

public class TracingRListMultimap<K, V> extends TracingRMultimap<K, V> implements
    RListMultimap<K, V> {
  private final RListMultimap<K, V> map;
  private final TracingHelper tracingHelper;

  public TracingRListMultimap(RListMultimap<K, V> map, TracingHelper tracingHelper) {
    super(map, tracingHelper);
    this.map = map;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public RList<V> get(K key) {
    Span span = tracingHelper.buildSpan("get", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> new TracingRList<>(map.get(key), tracingHelper));
  }

  @Override
  public List<V> getAll(K key) {
    Span span = tracingHelper.buildSpan("getAll", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.getAll(key));
  }

  @Override
  public List<V> removeAll(Object key) {
    Span span = tracingHelper.buildSpan("removeAll", map);
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> map.removeAll(key));
  }

  @Override
  public List<V> replaceValues(K key, Iterable<? extends V> values) {
    Span span = tracingHelper.buildSpan("replaceValues", map);
    span.setTag("key", nullable(key));
    span.setTag("values", nullable(values));
    return tracingHelper.decorate(span, () -> map.replaceValues(key, values));
  }

}
