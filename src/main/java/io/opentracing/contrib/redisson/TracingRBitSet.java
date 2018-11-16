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
import java.util.BitSet;
import org.redisson.api.RBitSet;
import org.redisson.api.RFuture;

public class TracingRBitSet extends TracingRExpirable implements RBitSet {
  private final RBitSet bitSet;
  private final TracingHelper tracingHelper;

  public TracingRBitSet(RBitSet bitSet, TracingHelper tracingHelper) {
    super(bitSet, tracingHelper);
    this.bitSet = bitSet;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public long length() {
    Span span = tracingHelper.buildSpan("length", bitSet);
    return tracingHelper.decorate(span, bitSet::length);
  }

  @Override
  public void set(long fromIndex, long toIndex, boolean value) {
    Span span = tracingHelper.buildSpan("set", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    span.setTag("value", value);
    tracingHelper.decorate(span, () -> bitSet.set(fromIndex, toIndex, value));
  }

  @Override
  public void clear(long fromIndex, long toIndex) {
    Span span = tracingHelper.buildSpan("clear", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    tracingHelper.decorate(span, () -> bitSet.clear(fromIndex, toIndex));
  }

  @Override
  public void set(BitSet bs) {
    Span span = tracingHelper.buildSpan("set", bitSet);
    span.setTag("bs", nullable(bs));
    tracingHelper.decorate(span, () -> bitSet.set(bs));
  }

  @Override
  public void not() {
    Span span = tracingHelper.buildSpan("not", bitSet);
    tracingHelper.decorate(span, bitSet::not);
  }

  @Override
  public void set(long fromIndex, long toIndex) {
    Span span = tracingHelper.buildSpan("set", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    tracingHelper.decorate(span, () -> bitSet.set(fromIndex, toIndex));
  }

  @Override
  public long size() {
    Span span = tracingHelper.buildSpan("size", bitSet);
    return tracingHelper.decorate(span, bitSet::size);
  }

  @Override
  public boolean get(long bitIndex) {
    Span span = tracingHelper.buildSpan("get", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingHelper.decorate(span, () -> bitSet.get(bitIndex));
  }

  @Override
  public boolean set(long bitIndex) {
    Span span = tracingHelper.buildSpan("set", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingHelper.decorate(span, () -> bitSet.set(bitIndex));
  }

  @Override
  public void set(long bitIndex, boolean value) {
    Span span = tracingHelper.buildSpan("set", bitSet);
    span.setTag("bitIndex", bitIndex);
    span.setTag("value", value);
    tracingHelper.decorate(span, () -> bitSet.set(bitIndex, value));
  }

  @Override
  public byte[] toByteArray() {
    Span span = tracingHelper.buildSpan("toByteArray", bitSet);
    return tracingHelper.decorate(span, bitSet::toByteArray);
  }

  @Override
  public long cardinality() {
    Span span = tracingHelper.buildSpan("cardinality", bitSet);
    return tracingHelper.decorate(span, bitSet::cardinality);
  }

  @Override
  public boolean clear(long bitIndex) {
    Span span = tracingHelper.buildSpan("clear", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingHelper.decorate(span, () -> bitSet.clear(bitIndex));
  }

  @Override
  public void clear() {
    Span span = tracingHelper.buildSpan("clear", bitSet);
    tracingHelper.decorate(span, () -> bitSet.clear());
  }

  @Override
  public BitSet asBitSet() {
    Span span = tracingHelper.buildSpan("asBitSet", bitSet);
    return tracingHelper.decorate(span, bitSet::asBitSet);
  }

  @Override
  public void or(String... bitSetNames) {
    Span span = tracingHelper.buildSpan("or", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    tracingHelper.decorate(span, () -> bitSet.or(bitSetNames));
  }

  @Override
  public void and(String... bitSetNames) {
    Span span = tracingHelper.buildSpan("and", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    tracingHelper.decorate(span, () -> bitSet.and(bitSetNames));
  }

  @Override
  public void xor(String... bitSetNames) {
    Span span = tracingHelper.buildSpan("xor", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    tracingHelper.decorate(span, () -> bitSet.xor(bitSetNames));
  }

  @Override
  public RFuture<byte[]> toByteArrayAsync() {
    Span span = tracingHelper.buildSpan("toByteArrayAsync", bitSet);
    return tracingHelper.prepareRFuture(span, bitSet::toByteArrayAsync);
  }

  @Override
  public RFuture<Long> lengthAsync() {
    Span span = tracingHelper.buildSpan("lengthAsync", bitSet);
    return tracingHelper.prepareRFuture(span, bitSet::lengthAsync);
  }

  @Override
  public RFuture<Void> setAsync(long fromIndex, long toIndex, boolean value) {
    Span span = tracingHelper.buildSpan("setAsync", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    span.setTag("value", value);
    return tracingHelper.prepareRFuture(span, () -> bitSet.setAsync(fromIndex, toIndex, value));
  }

  @Override
  public RFuture<Void> clearAsync(long fromIndex, long toIndex) {
    Span span = tracingHelper.buildSpan("clearAsync", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return tracingHelper.prepareRFuture(span, () -> bitSet.clearAsync(fromIndex, toIndex));
  }

  @Override
  public RFuture<Void> setAsync(BitSet bs) {
    Span span = tracingHelper.buildSpan("setAsync", bitSet);
    span.setTag("bs", nullable(bs));
    return tracingHelper.prepareRFuture(span, () -> bitSet.setAsync(bs));
  }

  @Override
  public RFuture<Void> notAsync() {
    Span span = tracingHelper.buildSpan("notAsync", bitSet);
    return tracingHelper.prepareRFuture(span, bitSet::notAsync);
  }

  @Override
  public RFuture<Void> setAsync(long fromIndex, long toIndex) {
    Span span = tracingHelper.buildSpan("setAsync", bitSet);
    span.setTag("fromIndex", fromIndex);
    span.setTag("toIndex", toIndex);
    return tracingHelper.prepareRFuture(span, () -> bitSet.setAsync(fromIndex, toIndex));
  }

  @Override
  public RFuture<Long> sizeAsync() {
    Span span = tracingHelper.buildSpan("sizeAsync", bitSet);
    return tracingHelper.prepareRFuture(span, bitSet::sizeAsync);
  }

  @Override
  public RFuture<Boolean> getAsync(long bitIndex) {
    Span span = tracingHelper.buildSpan("getAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingHelper.prepareRFuture(span, () -> bitSet.getAsync(bitIndex));
  }

  @Override
  public RFuture<Boolean> setAsync(long bitIndex) {
    Span span = tracingHelper.buildSpan("setAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingHelper.prepareRFuture(span, () -> bitSet.setAsync(bitIndex));
  }

  @Override
  public RFuture<Boolean> setAsync(long bitIndex, boolean value) {
    Span span = tracingHelper.buildSpan("setAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    span.setTag("value", value);
    return tracingHelper.prepareRFuture(span, () -> bitSet.setAsync(bitIndex, value));
  }

  @Override
  public RFuture<Long> cardinalityAsync() {
    Span span = tracingHelper.buildSpan("cardinalityAsync", bitSet);
    return tracingHelper.prepareRFuture(span, bitSet::cardinalityAsync);
  }

  @Override
  public RFuture<Boolean> clearAsync(long bitIndex) {
    Span span = tracingHelper.buildSpan("clearAsync", bitSet);
    span.setTag("bitIndex", bitIndex);
    return tracingHelper.prepareRFuture(span, () -> bitSet.clearAsync(bitIndex));
  }

  @Override
  public RFuture<Void> clearAsync() {
    Span span = tracingHelper.buildSpan("clearAsync", bitSet);
    return tracingHelper.prepareRFuture(span, bitSet::clearAsync);
  }

  @Override
  public RFuture<Void> orAsync(String... bitSetNames) {
    Span span = tracingHelper.buildSpan("orAsync", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    return tracingHelper.prepareRFuture(span, () -> bitSet.orAsync(bitSetNames));
  }

  @Override
  public RFuture<Void> andAsync(String... bitSetNames) {
    Span span = tracingHelper.buildSpan("andAsync", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    return tracingHelper.prepareRFuture(span, () -> bitSet.andAsync(bitSetNames));
  }

  @Override
  public RFuture<Void> xorAsync(String... bitSetNames) {
    Span span = tracingHelper.buildSpan("xorAsync", bitSet);
    span.setTag("bitSetNames", Arrays.toString(bitSetNames));
    return tracingHelper.prepareRFuture(span, () -> bitSet.xorAsync(bitSetNames));
  }

}
