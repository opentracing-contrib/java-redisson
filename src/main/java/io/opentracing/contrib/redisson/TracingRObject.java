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
import java.util.concurrent.TimeUnit;
import org.redisson.api.RFuture;
import org.redisson.api.RObject;
import org.redisson.client.codec.Codec;

public class TracingRObject implements RObject {
  private final RObject object;
  private final TracingHelper tracingHelper;

  public TracingRObject(RObject object, TracingHelper tracingHelper) {
    this.object = object;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public void restore(byte[] state) {
    Span span = tracingHelper.buildSpan("restore", object);
    span.setTag("state", Arrays.toString(state));
    tracingHelper.decorate(span, () -> object.restore(state));
  }

  @Override
  public void restore(byte[] state, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("restore", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    tracingHelper.decorate(span, () -> object.restore(state, timeToLive, timeUnit));
  }

  @Override
  public void restoreAndReplace(byte[] state) {
    Span span = tracingHelper.buildSpan("restoreAndReplace", object);
    span.setTag("state", Arrays.toString(state));
    tracingHelper.decorate(span, () -> object.restoreAndReplace(state));
  }

  @Override
  public void restoreAndReplace(byte[] state, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("restoreAndReplace", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    tracingHelper.decorate(span, () -> object.restoreAndReplace(state, timeToLive, timeUnit));
  }

  @Override
  public byte[] dump() {
    Span span = tracingHelper.buildSpan("dump", object);
    return tracingHelper.decorate(span, object::dump);
  }

  @Override
  public boolean touch() {
    Span span = tracingHelper.buildSpan("touch", object);
    return tracingHelper.decorate(span, object::touch);
  }

  @Override
  public void migrate(String host, int port, int database, long timeout) {
    Span span = tracingHelper.buildSpan("migrate", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingHelper.decorate(span, () -> object.migrate(host, port, database, timeout));
  }

  @Override
  public void copy(String host, int port, int database, long timeout) {
    Span span = tracingHelper.buildSpan("copy", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingHelper.decorate(span, () -> object.copy(host, port, database, timeout));
  }

  @Override
  public boolean move(int database) {
    Span span = tracingHelper.buildSpan("move", object);
    span.setTag("database", database);
    return tracingHelper.decorate(span, () -> object.move(database));
  }

  @Override
  public String getName() {
    return object.getName();
  }

  @Override
  public boolean delete() {
    Span span = tracingHelper.buildSpan("delete", object);
    return tracingHelper.decorate(span, object::delete);
  }

  @Override
  public boolean unlink() {
    Span span = tracingHelper.buildSpan("unlink", object);
    return tracingHelper.decorate(span, object::unlink);
  }

  @Override
  public void rename(String newName) {
    Span span = tracingHelper.buildSpan("rename", object);
    span.setTag("newName", nullable(newName));
    tracingHelper.decorate(span, () -> object.rename(newName));
  }

  @Override
  public boolean renamenx(String newName) {
    Span span = tracingHelper.buildSpan("renamenx", object);
    span.setTag("newName", nullable(newName));
    return tracingHelper.decorate(span, () -> object.renamenx(newName));
  }

  @Override
  public boolean isExists() {
    Span span = tracingHelper.buildSpan("isExists", object);
    return tracingHelper.decorate(span, object::isExists);
  }

  @Override
  public Codec getCodec() {
    return object.getCodec();
  }

  @Override
  public RFuture<Void> restoreAsync(byte[] state) {
    Span span = tracingHelper.buildSpan("restoreAsync", object);
    span.setTag("state", Arrays.toString(state));
    return tracingHelper.prepareRFuture(span, () -> object.restoreAsync(state));
  }

  @Override
  public RFuture<Void> restoreAsync(byte[] state, long timeToLive,
      TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("restoreAsync", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper
        .prepareRFuture(span, () -> object.restoreAsync(state, timeToLive, timeUnit));
  }

  @Override
  public RFuture<Void> restoreAndReplaceAsync(byte[] state) {
    Span span = tracingHelper.buildSpan("restoreAndReplaceAsync", object);
    span.setTag("state", Arrays.toString(state));
    return tracingHelper.prepareRFuture(span, () -> object.restoreAndReplaceAsync(state));
  }

  @Override
  public RFuture<Void> restoreAndReplaceAsync(byte[] state, long timeToLive,
      TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("restoreAndReplaceAsync", object);
    span.setTag("state", Arrays.toString(state));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper
        .prepareRFuture(span, () -> object.restoreAndReplaceAsync(state, timeToLive, timeUnit));
  }

  @Override
  public RFuture<byte[]> dumpAsync() {
    Span span = tracingHelper.buildSpan("dumpAsync", object);
    return tracingHelper.prepareRFuture(span, object::dumpAsync);
  }

  @Override
  public RFuture<Boolean> touchAsync() {
    Span span = tracingHelper.buildSpan("touchAsync", object);
    return tracingHelper.prepareRFuture(span, object::touchAsync);
  }

  @Override
  public RFuture<Void> migrateAsync(String host, int port, int database,
      long timeout) {
    Span span = tracingHelper.buildSpan("migrateAsync", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingHelper
        .prepareRFuture(span, () -> object.migrateAsync(host, port, database, timeout));
  }

  @Override
  public RFuture<Void> copyAsync(String host, int port, int database, long timeout) {
    Span span = tracingHelper.buildSpan("copyAsync", object);
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingHelper
        .prepareRFuture(span, () -> object.copyAsync(host, port, database, timeout));
  }

  @Override
  public RFuture<Boolean> moveAsync(int database) {
    Span span = tracingHelper.buildSpan("moveAsync", object);
    span.setTag("database", database);
    return tracingHelper.prepareRFuture(span, () -> object.moveAsync(database));
  }

  @Override
  public RFuture<Boolean> deleteAsync() {
    Span span = tracingHelper.buildSpan("deleteAsync", object);
    return tracingHelper.prepareRFuture(span, object::deleteAsync);
  }

  @Override
  public RFuture<Boolean> unlinkAsync() {
    Span span = tracingHelper.buildSpan("unlinkAsync", object);
    return tracingHelper.prepareRFuture(span, object::unlinkAsync);
  }

  @Override
  public RFuture<Void> renameAsync(String newName) {
    Span span = tracingHelper.buildSpan("renameAsync", object);
    span.setTag("newName", nullable(newName));
    return tracingHelper.prepareRFuture(span, () -> object.renameAsync(newName));
  }

  @Override
  public RFuture<Boolean> renamenxAsync(String newName) {
    Span span = tracingHelper.buildSpan("renamenxAsync", object);
    span.setTag("newName", nullable(newName));
    return tracingHelper.prepareRFuture(span, () -> object.renamenxAsync(newName));
  }

  @Override
  public RFuture<Boolean> isExistsAsync() {
    Span span = tracingHelper.buildSpan("isExistsAsync", object);
    return tracingHelper.prepareRFuture(span, object::isExistsAsync);
  }

  @Override
  public boolean equals(Object o) {
    Span span = tracingHelper.buildSpan("equals", object);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> object.equals(o));
  }

  @Override
  public int hashCode() {
    Span span = tracingHelper.buildSpan("hashCode", object);
    return tracingHelper.decorate(span, object::hashCode);
  }
}
