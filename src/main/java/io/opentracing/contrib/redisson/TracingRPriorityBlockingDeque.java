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
import org.redisson.api.RPriorityBlockingDeque;

public class TracingRPriorityBlockingDeque<V> extends TracingRBlockingDeque<V> implements
    RPriorityBlockingDeque<V> {
  private final RPriorityBlockingDeque<V> deque;
  private final TracingHelper tracingHelper;

  public TracingRPriorityBlockingDeque(RPriorityBlockingDeque<V> deque,
      TracingHelper tracingHelper) {
    super(deque, tracingHelper);
    this.deque = deque;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public Comparator<? super V> comparator() {
    return deque.comparator();
  }

  @Override
  public boolean trySetComparator(Comparator<? super V> comparator) {
    Span span = tracingHelper.buildSpan("trySetComparator", deque);
    span.setTag("comparator", nullable(comparator));
    return tracingHelper.decorate(span, () -> deque.trySetComparator(comparator));
  }

}
