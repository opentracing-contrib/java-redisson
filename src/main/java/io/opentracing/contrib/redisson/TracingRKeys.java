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
import java.util.concurrent.TimeUnit;
import org.redisson.api.RFuture;
import org.redisson.api.RKeys;
import org.redisson.api.RObject;
import org.redisson.api.RType;

public class TracingRKeys implements RKeys {
  private final RKeys keys;
  private final TracingHelper tracingHelper;

  public TracingRKeys(RKeys keys, TracingHelper tracingHelper) {
    this.keys = keys;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public boolean move(String name, int database) {
    Span span = tracingHelper.buildSpan("move");
    span.setTag("name", nullable(name));
    span.setTag("database", database);
    return tracingHelper.decorate(span, () -> keys.move(name, database));
  }

  @Override
  public void migrate(String name, String host, int port, int database, long timeout) {
    Span span = tracingHelper.buildSpan("migrate");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingHelper.decorate(span, () -> keys.migrate(name, host, port, database, timeout));
  }

  @Override
  public void copy(String name, String host, int port, int database, long timeout) {
    Span span = tracingHelper.buildSpan("copy");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    tracingHelper.decorate(span, () -> keys.copy(name, host, port, database, timeout));
  }

  @Override
  public boolean expire(String name, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("expire");
    span.setTag("name", nullable(name));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper.decorate(span, () -> keys.expire(name, timeToLive, timeUnit));
  }

  @Override
  public boolean expireAt(String name, long timestamp) {
    Span span = tracingHelper.buildSpan("expireAt");
    span.setTag("name", nullable(name));
    span.setTag("timestamp", timestamp);
    return tracingHelper.decorate(span, () -> keys.expireAt(name, timestamp));
  }

  @Override
  public boolean clearExpire(String name) {
    Span span = tracingHelper.buildSpan("clearExpire");
    span.setTag("name", nullable(name));
    return tracingHelper.decorate(span, () -> keys.clearExpire(name));
  }

  @Override
  public boolean renamenx(String oldName, String newName) {
    Span span = tracingHelper.buildSpan("renamenx");
    span.setTag("oldName", nullable(oldName));
    span.setTag("newName", nullable(newName));
    return tracingHelper.decorate(span, () -> keys.renamenx(oldName, newName));
  }

  @Override
  public void rename(String currentName, String newName) {
    Span span = tracingHelper.buildSpan("rename");
    span.setTag("currentName", nullable(currentName));
    span.setTag("newName", nullable(newName));
    tracingHelper.decorate(span, () -> keys.rename(currentName, newName));
  }

  @Override
  public long remainTimeToLive(String name) {
    Span span = tracingHelper.buildSpan("remainTimeToLive");
    span.setTag("name", nullable(name));
    return tracingHelper.decorate(span, () -> keys.remainTimeToLive(name));
  }

  @Override
  public long touch(String... names) {
    Span span = tracingHelper.buildSpan("touch");
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.decorate(span, () -> keys.touch(names));
  }

  @Override
  public long countExists(String... names) {
    Span span = tracingHelper.buildSpan("countExists");
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.decorate(span, () -> keys.countExists(names));
  }

  @Override
  public RType getType(String key) {
    Span span = tracingHelper.buildSpan("getType");
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> keys.getType(key));
  }

  @Override
  public int getSlot(String key) {
    Span span = tracingHelper.buildSpan("getSlot");
    span.setTag("key", nullable(key));
    return tracingHelper.decorate(span, () -> keys.getSlot(key));
  }

  @Override
  public Iterable<String> getKeysByPattern(String pattern) {
    Span span = tracingHelper.buildSpan("getKeysByPattern");
    span.setTag("pattern", nullable(pattern));
    return tracingHelper.decorate(span, () -> keys.getKeysByPattern(pattern));
  }

  @Override
  public Iterable<String> getKeysByPattern(String pattern, int count) {
    Span span = tracingHelper.buildSpan("getKeysByPattern");
    span.setTag("pattern", nullable(pattern));
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> keys.getKeysByPattern(pattern, count));
  }

  @Override
  public Iterable<String> getKeys() {
    Span span = tracingHelper.buildSpan("getKeys");
    return tracingHelper.decorate(span, () -> keys.getKeys());
  }

  @Override
  public Iterable<String> getKeys(int count) {
    Span span = tracingHelper.buildSpan("getKeys");
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> keys.getKeys(count));
  }

  @Override
  public String randomKey() {
    Span span = tracingHelper.buildSpan("randomKey");
    return tracingHelper.decorate(span, keys::randomKey);
  }

  @Override
  @Deprecated
  public Collection<String> findKeysByPattern(String pattern) {
    Span span = tracingHelper.buildSpan("findKeysByPattern");
    span.setTag("pattern", nullable(pattern));
    return tracingHelper.decorate(span, () -> keys.findKeysByPattern(pattern));
  }

  @Override
  public long deleteByPattern(String pattern) {
    Span span = tracingHelper.buildSpan("deleteByPattern");
    span.setTag("pattern", nullable(pattern));
    return tracingHelper.decorate(span, () -> keys.deleteByPattern(pattern));
  }

  @Override
  public long delete(RObject... objects) {
    Span span = tracingHelper.buildSpan("delete");
    span.setTag("objects", Arrays.toString(objects));
    return tracingHelper.decorate(span, () -> keys.delete(objects));
  }

  @Override
  public long delete(String... keys) {
    Span span = tracingHelper.buildSpan("delete");
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.decorate(span, () -> this.keys.delete(keys));
  }

  @Override
  public long unlink(String... keys) {
    Span span = tracingHelper.buildSpan("unlink");
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.decorate(span, () -> this.keys.unlink(keys));
  }

  @Override
  public long count() {
    Span span = tracingHelper.buildSpan("count");
    return tracingHelper.decorate(span, keys::count);
  }

  @Override
  public void flushdb() {
    Span span = tracingHelper.buildSpan("flushdb");
    tracingHelper.decorate(span, keys::flushdb);
  }

  @Override
  public void flushdbParallel() {
    Span span = tracingHelper.buildSpan("flushdbParallel");
    tracingHelper.decorate(span, keys::flushdbParallel);
  }

  @Override
  public void flushall() {
    Span span = tracingHelper.buildSpan("flushall");
    tracingHelper.decorate(span, keys::flushall);
  }

  @Override
  public void flushallParallel() {
    Span span = tracingHelper.buildSpan("flushallParallel");
    tracingHelper.decorate(span, keys::flushallParallel);
  }

  @Override
  public RFuture<Boolean> moveAsync(String name, int database) {
    Span span = tracingHelper.buildSpan("moveAsync");
    span.setTag("name", nullable(name));
    span.setTag("database", database);
    return tracingHelper.prepareRFuture(span, () -> keys.moveAsync(name, database));
  }

  @Override
  public RFuture<Void> migrateAsync(String name, String host, int port, int database,
      long timeout) {
    Span span = tracingHelper.buildSpan("migrateAsync");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingHelper
        .prepareRFuture(span, () -> keys.migrateAsync(name, host, port, database, timeout));
  }

  @Override
  public RFuture<Void> copyAsync(String name, String host, int port, int database, long timeout) {
    Span span = tracingHelper.buildSpan("copyAsync");
    span.setTag("name", nullable(name));
    span.setTag("host", nullable(host));
    span.setTag("port", port);
    span.setTag("database", database);
    span.setTag("timeout", timeout);
    return tracingHelper
        .prepareRFuture(span, () -> keys.copyAsync(name, host, port, database, timeout));
  }

  @Override
  public RFuture<Boolean> expireAsync(String name, long timeToLive, TimeUnit timeUnit) {
    Span span = tracingHelper.buildSpan("expireAsync");
    span.setTag("name", nullable(name));
    span.setTag("timeToLive", timeToLive);
    span.setTag("timeUnit", nullable(timeUnit));
    return tracingHelper.prepareRFuture(span, () -> keys.expireAsync(name, timeToLive, timeUnit));
  }

  @Override
  public RFuture<Boolean> expireAtAsync(String name, long timestamp) {
    Span span = tracingHelper.buildSpan("expireAtAsync");
    span.setTag("name", nullable(name));
    span.setTag("timestamp", timestamp);
    return tracingHelper.prepareRFuture(span, () -> keys.expireAtAsync(name, timestamp));
  }

  @Override
  public RFuture<Boolean> clearExpireAsync(String name) {
    Span span = tracingHelper.buildSpan("clearExpireAsync");
    span.setTag("name", nullable(name));
    return tracingHelper.prepareRFuture(span, () -> keys.clearExpireAsync(name));
  }

  @Override
  public RFuture<Boolean> renamenxAsync(String oldName, String newName) {
    Span span = tracingHelper.buildSpan("renamenxAsync");
    span.setTag("oldName", nullable(oldName));
    span.setTag("newName", nullable(newName));
    return tracingHelper.prepareRFuture(span, () -> keys.renamenxAsync(oldName, newName));
  }

  @Override
  public RFuture<Void> renameAsync(String currentName, String newName) {
    Span span = tracingHelper.buildSpan("renameAsync");
    span.setTag("currentName", nullable(currentName));
    span.setTag("newName", nullable(newName));
    return tracingHelper.prepareRFuture(span, () -> keys.renameAsync(currentName, newName));
  }

  @Override
  public RFuture<Long> remainTimeToLiveAsync(String name) {
    Span span = tracingHelper.buildSpan("remainTimeToLiveAsync");
    span.setTag("name", nullable(name));
    return tracingHelper.prepareRFuture(span, () -> keys.remainTimeToLiveAsync(name));
  }

  @Override
  public RFuture<Long> touchAsync(String... names) {
    Span span = tracingHelper.buildSpan("touchAsync");
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.prepareRFuture(span, () -> keys.touchAsync(names));
  }

  @Override
  public RFuture<Long> countExistsAsync(String... names) {
    Span span = tracingHelper.buildSpan("countExistsAsync");
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.prepareRFuture(span, () -> keys.countExistsAsync(names));
  }

  @Override
  public RFuture<RType> getTypeAsync(String key) {
    Span span = tracingHelper.buildSpan("getTypeAsync");
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> keys.getTypeAsync(key));
  }

  @Override
  public RFuture<Integer> getSlotAsync(String key) {
    Span span = tracingHelper.buildSpan("getSlotAsync");
    span.setTag("key", nullable(key));
    return tracingHelper.prepareRFuture(span, () -> keys.getSlotAsync(key));
  }

  @Override
  public RFuture<String> randomKeyAsync() {
    Span span = tracingHelper.buildSpan("randomKeyAsync");
    return tracingHelper.prepareRFuture(span, keys::randomKeyAsync);
  }

  @Override
  @Deprecated
  public RFuture<Collection<String>> findKeysByPatternAsync(String pattern) {
    Span span = tracingHelper.buildSpan("findKeysByPatternAsync");
    span.setTag("pattern", nullable(pattern));
    return tracingHelper.prepareRFuture(span, () -> keys.findKeysByPatternAsync(pattern));
  }

  @Override
  public RFuture<Long> deleteByPatternAsync(String pattern) {
    Span span = tracingHelper.buildSpan("deleteByPatternAsync");
    span.setTag("pattern", nullable(pattern));
    return tracingHelper.prepareRFuture(span, () -> keys.deleteByPatternAsync(pattern));
  }

  @Override
  public RFuture<Long> deleteAsync(RObject... objects) {
    Span span = tracingHelper.buildSpan("deleteAsync");
    span.setTag("objects", Arrays.toString(objects));
    return tracingHelper.prepareRFuture(span, () -> keys.deleteAsync(objects));
  }

  @Override
  public RFuture<Long> deleteAsync(String... keys) {
    Span span = tracingHelper.buildSpan("deleteAsync");
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.prepareRFuture(span, () -> this.keys.deleteAsync(keys));
  }

  @Override
  public RFuture<Long> unlinkAsync(String... keys) {
    Span span = tracingHelper.buildSpan("unlinkAsync");
    span.setTag("keys", Arrays.toString(keys));
    return tracingHelper.prepareRFuture(span, () -> this.keys.unlinkAsync(keys));
  }

  @Override
  public RFuture<Long> countAsync() {
    Span span = tracingHelper.buildSpan("countAsync");
    return tracingHelper.prepareRFuture(span, keys::countAsync);
  }

  @Override
  public RFuture<Void> flushdbAsync() {
    Span span = tracingHelper.buildSpan("flushdbAsync");
    return tracingHelper.prepareRFuture(span, keys::flushdbAsync);
  }

  @Override
  public RFuture<Void> flushallAsync() {
    Span span = tracingHelper.buildSpan("flushallAsync");
    return tracingHelper.prepareRFuture(span, keys::flushallAsync);
  }

  @Override
  public RFuture<Void> flushdbParallelAsync() {
    Span span = tracingHelper.buildSpan("flushdbParallelAsync");
    return tracingHelper.prepareRFuture(span, keys::flushdbParallelAsync);
  }

  @Override
  public RFuture<Void> flushallParallelAsync() {
    Span span = tracingHelper.buildSpan("flushallParallelAsync");
    return tracingHelper.prepareRFuture(span, keys::flushallParallelAsync);
  }

}
