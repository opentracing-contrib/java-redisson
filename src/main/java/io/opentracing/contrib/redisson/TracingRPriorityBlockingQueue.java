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
import java.util.Comparator;
import org.redisson.api.RPriorityBlockingQueue;

public class TracingRPriorityBlockingQueue<V> extends TracingRBlockingQueue<V> implements
    RPriorityBlockingQueue<V> {
  private final RPriorityBlockingQueue<V> queue;
  private final TracingHelper tracingHelper;

  public TracingRPriorityBlockingQueue(RPriorityBlockingQueue<V> queue,
      TracingHelper tracingHelper) {
    super(queue, tracingHelper);
    this.queue = queue;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public Comparator<? super V> comparator() {
    return queue.comparator();
  }

  @Override
  public boolean trySetComparator(Comparator<? super V> comparator) {
    Span span = tracingHelper.buildSpan("trySetComparator", queue);
    span.setTag("comparator", nullable(comparator));
    return tracingHelper.decorate(span, () -> queue.trySetComparator(comparator));
  }
}
