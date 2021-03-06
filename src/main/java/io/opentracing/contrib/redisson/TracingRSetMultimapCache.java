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
import org.redisson.api.RFuture;
import org.redisson.api.RSetMultimapCache;

public class TracingRSetMultimapCache<K, V> extends TracingRSetMultimap<K, V> implements
    RSetMultimapCache<K, V> {
  private final RSetMultimapCache<K, V> cache;
  private final TracingHelper tracingHelper;

  public TracingRSetMultimapCache(RSetMultimapCache<K, V> cache, TracingHelper tracingHelper) {
    super(cache, tracingHelper);
    this.cache = cache;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public boolean expireKey(K key, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("expireKey", cache);
    span.setTag("key", nullable(key));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper.decorate(span, () -> cache.expireKey(key, timeToLive, timeUnit));
  }

  @Override
  public RFuture<Boolean> expireKeyAsync(K key, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("expireKeyAsync", cache);
    span.setTag("key", nullable(key));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper
        .prepareRFuture(span, () -> cache.expireKeyAsync(key, timeToLive, timeUnit));
  }

}
