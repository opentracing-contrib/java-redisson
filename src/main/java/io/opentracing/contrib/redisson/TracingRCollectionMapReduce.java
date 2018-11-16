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

import java.util.concurrent.TimeUnit;
import org.redisson.api.mapreduce.RCollectionMapReduce;
import org.redisson.api.mapreduce.RCollectionMapper;
import org.redisson.api.mapreduce.RReducer;

public class TracingRCollectionMapReduce<VIn, KOut, VOut> extends
    TracingRMapReduceExecutor<VIn, KOut, VOut> implements RCollectionMapReduce<VIn, KOut, VOut> {
  private final RCollectionMapReduce<VIn, KOut, VOut> mapReduce;
  private final TracingHelper tracingHelper;

  public TracingRCollectionMapReduce(RCollectionMapReduce<VIn, KOut, VOut> mapReduce,
      TracingHelper tracingHelper) {
    super(mapReduce, tracingHelper);
    this.mapReduce = mapReduce;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public RCollectionMapReduce<VIn, KOut, VOut> timeout(long timeout, TimeUnit unit) {
    return new TracingRCollectionMapReduce<>(mapReduce.timeout(timeout, unit), tracingHelper);
  }

  @Override
  public RCollectionMapReduce<VIn, KOut, VOut> mapper(
      RCollectionMapper<VIn, KOut, VOut> mapper) {
    return new TracingRCollectionMapReduce<>(mapReduce.mapper(mapper), tracingHelper);
  }

  @Override
  public RCollectionMapReduce<VIn, KOut, VOut> reducer(
      RReducer<KOut, VOut> reducer) {
    return new TracingRCollectionMapReduce<>(mapReduce.reducer(reducer), tracingHelper);
  }

}
