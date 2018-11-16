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

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.redisson.api.RFuture;
import org.redisson.api.RObject;

class TracingHelper {
  static final String COMPONENT_NAME = "java-redisson";
  static final String DB_TYPE = "redis";
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;

  TracingHelper(Tracer tracer, boolean traceWithActiveSpanOnly) {
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }


  Span buildSpan(String operationName, RObject rObject) {
    if (traceWithActiveSpanOnly && getNullSafeTracer().activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, null).start()
          .setTag("name", rObject.getName());
    }
  }

  Span buildSpan(String operationName) {
    if (traceWithActiveSpanOnly && getNullSafeTracer().activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName, null).start();
    }
  }

  private SpanBuilder builder(String operationName, SpanContext parent) {
    SpanBuilder builder = getNullSafeTracer().buildSpan(operationName)
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.DB_TYPE.getKey(), DB_TYPE);
    if (parent != null) {
      builder.asChildOf(parent);
    }
    return builder;
  }


  <T> T decorate(Span span, Supplier<T> supplier) {
    try (Scope ignore = getNullSafeTracer().scopeManager().activate(span, false)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  void decorate(Span span, Action action) {
    try (Scope ignore = getNullSafeTracer().scopeManager().activate(span, false)) {
      action.execute();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  <T extends Exception> void decorateThrowing(Span span, ThrowingAction<T> action) throws T {
    try (Scope ignore = getNullSafeTracer().scopeManager().activate(span, false)) {
      action.execute();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  <T extends Exception, V> V decorateThrowing(Span span, ThrowingSupplier<T, V> supplier) throws T {
    try (Scope ignore = getNullSafeTracer().scopeManager().activate(span, false)) {
      return supplier.get();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  private static void onError(Throwable throwable, Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);

    if (throwable != null) {
      span.log(errorLogs(throwable));
    }
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(2);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.object", throwable);
    return errorLogs;
  }

  private Tracer getNullSafeTracer() {
    if (tracer == null) {
      return GlobalTracer.get();
    }
    return tracer;
  }

  static String collectionToString(Collection<?> collection) {
    if (collection == null) {
      return "";
    }
    return collection.stream().map(Object::toString).collect(Collectors.joining(", "));
  }

  static <K, V> String mapToString(Map<K, V> map) {
    if (map == null) {
      return "";
    }
    return map.entrySet()
        .stream()
        .map(entry -> entry.getKey() + " -> " + entry.getValue())
        .collect(Collectors.joining(", "));
  }

  static String nullable(Object object) {
    return object == null ? "" : object.toString();
  }

  private <T> RFuture<T> continueScopeSpan(RFuture<T> redisFuture) {
    Tracer tracer = getNullSafeTracer();
    Span span = tracer.activeSpan();
    CompletableRFuture<T> customRedisFuture = new CompletableRFuture<>(redisFuture);
    redisFuture.whenComplete((v, throwable) -> {
      try (Scope ignored = tracer.scopeManager().activate(span, false)) {
        if (throwable != null) {
          customRedisFuture.completeExceptionally(throwable);
        } else {
          customRedisFuture.complete(v);
        }
      }
    });
    return customRedisFuture;
  }

  private <V> RFuture<V> setCompleteAction(RFuture<V> future, Span span) {
    future.whenComplete((v, throwable) -> {
      if (throwable != null) {
        onError(throwable, span);
      }
      span.finish();
    });

    return future;
  }

  <V> RFuture<V> prepareRFuture(Span span, Supplier<RFuture<V>> futureSupplier) {
    RFuture<V> future;
    try {
      future = futureSupplier.get();
    } catch (Exception e) {
      onError(e, span);
      span.finish();
      throw e;
    }

    return continueScopeSpan(setCompleteAction(future, span));
  }
}
