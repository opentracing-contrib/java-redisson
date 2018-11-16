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

import static io.opentracing.contrib.redisson.TracingHelper.collectionToString;
import static io.opentracing.contrib.redisson.TracingHelper.nullable;

import io.opentracing.Span;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.redisson.api.RFuture;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.SortOrder;
import org.redisson.api.mapreduce.RCollectionMapReduce;
import org.redisson.client.protocol.ScoredEntry;

public class TracingRScoredSortedSet<V> extends TracingRExpirable implements RScoredSortedSet<V> {
  private final RScoredSortedSet<V> set;
  private final TracingHelper tracingHelper;

  public TracingRScoredSortedSet(RScoredSortedSet<V> set, TracingHelper tracingHelper) {
    super(set, tracingHelper);
    this.set = set;
    this.tracingHelper = tracingHelper;
  }

  @Override
  public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
    return new TracingRCollectionMapReduce<>(set.mapReduce(), tracingHelper);
  }

  @Override
  public V pollLastFromAny(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollLastFromAny", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper.decorate(span, () -> set.pollLastFromAny(timeout, unit, queueNames));
  }

  @Override
  public V pollFirstFromAny(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollFirstFromAny", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper.decorate(span, () -> set.pollFirstFromAny(timeout, unit, queueNames));
  }

  @Override
  public V pollFirst(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollFirst", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorate(span, () -> set.pollFirst(timeout, unit));
  }

  @Override
  public V pollLast(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollLast", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.decorate(span, () -> set.pollLast(timeout, unit));
  }

  @Override
  public Collection<V> pollFirst(int count) {
    Span span = tracingHelper.buildSpan("pollFirst", set);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> set.pollFirst(count));
  }

  @Override
  public Collection<V> pollLast(int count) {
    Span span = tracingHelper.buildSpan("pollLast", set);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> set.pollLast(count));
  }

  @Override
  public V pollFirst() {
    Span span = tracingHelper.buildSpan("pollFirst", set);
    return tracingHelper.decorate(span, () -> set.pollFirst());
  }

  @Override
  public V pollLast() {
    Span span = tracingHelper.buildSpan("pollLast", set);
    return tracingHelper.decorate(span, () -> set.pollLast());
  }

  @Override
  public V first() {
    Span span = tracingHelper.buildSpan("first", set);
    return tracingHelper.decorate(span, set::first);
  }

  @Override
  public V last() {
    Span span = tracingHelper.buildSpan("last", set);
    return tracingHelper.decorate(span, set::last);
  }

  @Override
  public Double firstScore() {
    Span span = tracingHelper.buildSpan("firstScore", set);
    return tracingHelper.decorate(span, set::firstScore);
  }

  @Override
  public Double lastScore() {
    Span span = tracingHelper.buildSpan("lastScore", set);
    return tracingHelper.decorate(span, set::lastScore);
  }

  @Override
  public int addAll(Map<V, Double> objects) {
    Span span = tracingHelper.buildSpan("addAll", set);
    return tracingHelper.decorate(span, () -> set.addAll(objects));
  }

  @Override
  public int removeRangeByScore(double startScore, boolean startScoreInclusive, double endScore,
      boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("removeRangeByScore", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.decorate(span,
        () -> set.removeRangeByScore(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public int removeRangeByRank(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("removeRangeByRank", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper.decorate(span, () -> set.removeRangeByRank(startIndex, endIndex));
  }

  @Override
  public Integer rank(V o) {
    Span span = tracingHelper.buildSpan("rank", set);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> set.rank(o));
  }

  @Override
  public Integer revRank(V o) {
    Span span = tracingHelper.buildSpan("revRank", set);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> set.revRank(o));
  }

  @Override
  public Double getScore(V o) {
    Span span = tracingHelper.buildSpan("getScore", set);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> set.getScore(o));
  }

  @Override
  public boolean add(double score, V object) {
    Span span = tracingHelper.buildSpan("add", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.decorate(span, () -> set.add(score, object));
  }

  @Override
  public Integer addAndGetRank(double score, V object) {
    Span span = tracingHelper.buildSpan("addAndGetRank", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.decorate(span, () -> set.addAndGetRank(score, object));
  }

  @Override
  public Integer addAndGetRevRank(double score, V object) {
    Span span = tracingHelper.buildSpan("addAndGetRevRank", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.decorate(span, () -> set.addAndGetRevRank(score, object));
  }

  @Override
  public boolean tryAdd(double score, V object) {
    Span span = tracingHelper.buildSpan("tryAdd", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.decorate(span, () -> set.tryAdd(score, object));
  }

  @Override
  public int size() {
    Span span = tracingHelper.buildSpan("size", set);
    return tracingHelper.decorate(span, set::size);
  }

  @Override
  public boolean isEmpty() {
    Span span = tracingHelper.buildSpan("isEmpty", set);
    return tracingHelper.decorate(span, set::isEmpty);
  }

  @Override
  public Iterator<V> iterator(String pattern) {
    return set.iterator(pattern);
  }

  @Override
  public Iterator<V> iterator(int count) {
    return set.iterator(count);
  }

  @Override
  public Iterator<V> iterator(String pattern, int count) {
    return set.iterator(pattern, count);
  }

  @Override
  public boolean contains(Object o) {
    Span span = tracingHelper.buildSpan("contains", set);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> set.contains(o));
  }

  @Override
  public Object[] toArray() {
    Span span = tracingHelper.buildSpan("toArray", set);
    return tracingHelper.decorate(span, () -> set.toArray());
  }

  @Override
  public <T> T[] toArray(T[] a) {
    Span span = tracingHelper.buildSpan("toArray", set);
    return tracingHelper.decorate(span, () -> set.toArray(a));
  }

  @Override
  public boolean remove(Object o) {
    Span span = tracingHelper.buildSpan("remove", set);
    span.setTag("object", nullable(o));
    return tracingHelper.decorate(span, () -> set.remove(o));
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("containsAll", set);
    return tracingHelper.decorate(span, () -> set.containsAll(c));
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("removeAll", set);
    return tracingHelper.decorate(span, () -> set.removeAll(c));
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    Span span = tracingHelper.buildSpan("retainAll", set);
    return tracingHelper.decorate(span, () -> set.retainAll(c));
  }

  @Override
  public void clear() {
    Span span = tracingHelper.buildSpan("clear", set);
    tracingHelper.decorate(span, set::clear);
  }

  @Override
  public Double addScore(V object, Number value) {
    Span span = tracingHelper.buildSpan("addScore", set);
    span.setTag("object", nullable(object));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> set.addScore(object, value));
  }

  @Override
  public Integer addScoreAndGetRank(V object, Number value) {
    Span span = tracingHelper.buildSpan("addScoreAndGetRank", set);
    span.setTag("object", nullable(object));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> set.addScoreAndGetRank(object, value));
  }

  @Override
  public Integer addScoreAndGetRevRank(V object, Number value) {
    Span span = tracingHelper.buildSpan("addScoreAndGetRevRank", set);
    span.setTag("object", nullable(object));
    span.setTag("value", nullable(value));
    return tracingHelper.decorate(span, () -> set.addScoreAndGetRevRank(object, value));
  }

  @Override
  public Collection<V> valueRange(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("valueRange", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper.decorate(span, () -> set.valueRange(startIndex, endIndex));
  }

  @Override
  public Collection<V> valueRangeReversed(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("valueRangeReversed", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper.decorate(span, () -> set.valueRangeReversed(startIndex, endIndex));
  }

  @Override
  public Collection<ScoredEntry<V>> entryRange(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("entryRange", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper.decorate(span, () -> set.entryRange(startIndex, endIndex));
  }

  @Override
  public Collection<ScoredEntry<V>> entryRangeReversed(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("entryRangeReversed", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper.decorate(span, () -> set.entryRangeReversed(startIndex, endIndex));
  }

  @Override
  public Collection<V> valueRange(double startScore, boolean startScoreInclusive,
      double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("valueRange", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.decorate(span,
        () -> set.valueRange(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public Collection<V> valueRangeReversed(double startScore, boolean startScoreInclusive,
      double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("valueRangeReversed", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.decorate(span,
        () -> set.valueRangeReversed(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public Collection<ScoredEntry<V>> entryRange(double startScore, boolean startScoreInclusive,
      double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("entryRange", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.decorate(span,
        () -> set.entryRange(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public Collection<V> valueRange(double startScore, boolean startScoreInclusive,
      double endScore, boolean endScoreInclusive, int offset, int count) {
    Span span = tracingHelper.buildSpan("valueRange", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span,
        () -> set.valueRange(startScore, startScoreInclusive, endScore, endScoreInclusive, offset,
            count));
  }

  @Override
  public Collection<V> valueRangeReversed(double startScore, boolean startScoreInclusive,
      double endScore, boolean endScoreInclusive, int offset, int count) {
    Span span = tracingHelper.buildSpan("valueRangeReversed", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span,
        () -> set.valueRangeReversed(startScore, startScoreInclusive, endScore, endScoreInclusive,
            offset, count));
  }

  @Override
  public Collection<ScoredEntry<V>> entryRange(
      double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive,
      int offset, int count) {
    Span span = tracingHelper.buildSpan("entryRange", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span,
        () -> set.entryRange(startScore, startScoreInclusive, endScore, endScoreInclusive, offset,
            count));
  }

  @Override
  public Collection<ScoredEntry<V>> entryRangeReversed(
      double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("entryRangeReversed", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.decorate(span,
        () -> set.entryRangeReversed(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public Collection<ScoredEntry<V>> entryRangeReversed(
      double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive,
      int offset, int count) {
    Span span = tracingHelper.buildSpan("entryRangeReversed", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span,
        () -> set.entryRangeReversed(startScore, startScoreInclusive, endScore, endScoreInclusive,
            offset, count));
  }

  @Override
  public int count(double startScore, boolean startScoreInclusive, double endScore,
      boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("count", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.decorate(span,
        () -> set.count(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public Collection<V> readAll() {
    Span span = tracingHelper.buildSpan("readAll", set);
    return tracingHelper.decorate(span, set::readAll);
  }

  @Override
  public int intersection(String... names) {
    Span span = tracingHelper.buildSpan("intersection", set);
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.decorate(span, () -> set.intersection(names));
  }

  @Override
  public int intersection(Aggregate aggregate, String... names) {
    Span span = tracingHelper.buildSpan("intersection", set);
    span.setTag("aggregate", nullable(aggregate));
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.decorate(span, () -> set.intersection(aggregate, names));
  }

  @Override
  public int intersection(Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("intersection", set);
    return tracingHelper.decorate(span, () -> set.intersection(nameWithWeight));
  }

  @Override
  public int intersection(Aggregate aggregate, Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("intersection", set);
    span.setTag("aggregate", nullable(aggregate));
    return tracingHelper.decorate(span, () -> set.intersection(aggregate, nameWithWeight));
  }

  @Override
  public int union(String... names) {
    Span span = tracingHelper.buildSpan("union", set);
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.decorate(span, () -> set.union(names));
  }

  @Override
  public int union(Aggregate aggregate, String... names) {
    Span span = tracingHelper.buildSpan("union", set);
    span.setTag("aggregate", nullable(aggregate));
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.decorate(span, () -> set.union(aggregate, names));
  }

  @Override
  public int union(Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("union", set);
    return tracingHelper.decorate(span, () -> set.union(nameWithWeight));
  }

  @Override
  public int union(Aggregate aggregate, Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("union", set);
    span.setTag("aggregate", nullable(aggregate));
    return tracingHelper.decorate(span, () -> set.union(aggregate, nameWithWeight));
  }

  @Override
  public RFuture<V> pollLastFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollLastFromAnyAsync", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .prepareRFuture(span, () -> set.pollLastFromAnyAsync(timeout, unit, queueNames));
  }

  @Override
  public RFuture<V> pollFirstFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
    Span span = tracingHelper.buildSpan("pollFirstFromAnyAsync", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    span.setTag("queueNames", Arrays.toString(queueNames));
    return tracingHelper
        .prepareRFuture(span, () -> set.pollFirstFromAnyAsync(timeout, unit, queueNames));
  }

  @Override
  public RFuture<V> pollFirstAsync(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollFirstAsync", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> set.pollFirstAsync(timeout, unit));
  }

  @Override
  public RFuture<V> pollLastAsync(long timeout, TimeUnit unit) {
    Span span = tracingHelper.buildSpan("pollLastAsync", set);
    span.setTag("timeout", timeout);
    span.setTag("unit", nullable(unit));
    return tracingHelper.prepareRFuture(span, () -> set.pollLastAsync(timeout, unit));
  }

  @Override
  public RFuture<Collection<V>> pollFirstAsync(int count) {
    Span span = tracingHelper.buildSpan("pollFirstAsync", set);
    span.setTag("count", count);
    return tracingHelper.prepareRFuture(span, () -> set.pollFirstAsync(count));
  }

  @Override
  public RFuture<Collection<V>> pollLastAsync(int count) {
    Span span = tracingHelper.buildSpan("pollLastAsync", set);
    span.setTag("count", count);
    return tracingHelper.prepareRFuture(span, () -> set.pollLastAsync(count));
  }

  @Override
  public RFuture<V> pollFirstAsync() {
    Span span = tracingHelper.buildSpan("pollFirstAsync", set);
    return tracingHelper.prepareRFuture(span, set::pollFirstAsync);
  }

  @Override
  public RFuture<V> pollLastAsync() {
    Span span = tracingHelper.buildSpan("pollLastAsync", set);
    return tracingHelper.prepareRFuture(span, set::pollLastAsync);
  }

  @Override
  public RFuture<V> firstAsync() {
    Span span = tracingHelper.buildSpan("firstAsync", set);
    return tracingHelper.prepareRFuture(span, set::firstAsync);
  }

  @Override
  public RFuture<V> lastAsync() {
    Span span = tracingHelper.buildSpan("lastAsync", set);
    return tracingHelper.prepareRFuture(span, set::lastAsync);
  }

  @Override
  public RFuture<Double> firstScoreAsync() {
    Span span = tracingHelper.buildSpan("firstScoreAsync", set);
    return tracingHelper.prepareRFuture(span, set::firstScoreAsync);
  }

  @Override
  public RFuture<Double> lastScoreAsync() {
    Span span = tracingHelper.buildSpan("lastScoreAsync", set);
    return tracingHelper.prepareRFuture(span, set::lastScoreAsync);
  }

  @Override
  public RFuture<Integer> addAllAsync(Map<V, Double> objects) {
    Span span = tracingHelper.buildSpan("addAllAsync", set);
    return tracingHelper.prepareRFuture(span, () -> set.addAllAsync(objects));
  }

  @Override
  public RFuture<Integer> removeRangeByScoreAsync(double startScore,
      boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("removeRangeByScoreAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.prepareRFuture(span, () -> set
        .removeRangeByScoreAsync(startScore, startScoreInclusive, endScore, endScoreInclusive)
    );

  }

  @Override
  public RFuture<Integer> removeRangeByRankAsync(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("removeRangeByRankAsync", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper
        .prepareRFuture(span, () -> set.removeRangeByRankAsync(startIndex, endIndex));
  }

  @Override
  public RFuture<Integer> rankAsync(V o) {
    Span span = tracingHelper.buildSpan("rankAsync", set);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> set.rankAsync(o));
  }

  @Override
  public RFuture<Integer> revRankAsync(V o) {
    Span span = tracingHelper.buildSpan("revRankAsync", set);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> set.revRankAsync(o));
  }

  @Override
  public RFuture<Double> getScoreAsync(V o) {
    Span span = tracingHelper.buildSpan("getScoreAsync", set);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> set.getScoreAsync(o));
  }

  @Override
  public RFuture<Boolean> addAsync(double score, V object) {
    Span span = tracingHelper.buildSpan("addAsync", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.prepareRFuture(span, () -> set.addAsync(score, object));
  }

  @Override
  public RFuture<Integer> addAndGetRankAsync(double score, V object) {
    Span span = tracingHelper.buildSpan("addAndGetRankAsync", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.prepareRFuture(span, () -> set.addAndGetRankAsync(score, object));
  }

  @Override
  public RFuture<Integer> addAndGetRevRankAsync(double score, V object) {
    Span span = tracingHelper.buildSpan("addAndGetRevRankAsync", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.prepareRFuture(span, () -> set.addAndGetRevRankAsync(score, object));
  }

  @Override
  public RFuture<Boolean> tryAddAsync(double score, V object) {
    Span span = tracingHelper.buildSpan("tryAddAsync", set);
    span.setTag("score", score);
    span.setTag("object", nullable(object));
    return tracingHelper.prepareRFuture(span, () -> set.tryAddAsync(score, object));
  }

  @Override
  public RFuture<Boolean> removeAsync(V object) {
    Span span = tracingHelper.buildSpan("removeAsync", set);
    span.setTag("object", nullable(object));
    return tracingHelper.prepareRFuture(span, () -> set.removeAsync(object));
  }

  @Override
  public RFuture<Integer> sizeAsync() {
    Span span = tracingHelper.buildSpan("sizeAsync", set);
    return tracingHelper.prepareRFuture(span, set::sizeAsync);
  }

  @Override
  public RFuture<Boolean> containsAsync(Object o) {
    Span span = tracingHelper.buildSpan("containsAsync", set);
    span.setTag("object", nullable(o));
    return tracingHelper.prepareRFuture(span, () -> set.containsAsync(o));
  }

  @Override
  public RFuture<Boolean> containsAllAsync(Collection<?> c) {
    Span span = tracingHelper.buildSpan("containsAllAsync", set);
    return tracingHelper.prepareRFuture(span, () -> set.containsAllAsync(c));
  }

  @Override
  public RFuture<Boolean> removeAllAsync(Collection<?> c) {
    Span span = tracingHelper.buildSpan("removeAllAsync", set);
    return tracingHelper.prepareRFuture(span, () -> set.removeAllAsync(c));
  }

  @Override
  public RFuture<Boolean> retainAllAsync(Collection<?> c) {
    Span span = tracingHelper.buildSpan("retainAllAsync", set);
    return tracingHelper.prepareRFuture(span, () -> set.retainAllAsync(c));
  }

  @Override
  public RFuture<Double> addScoreAsync(V object, Number value) {
    Span span = tracingHelper.buildSpan("addScoreAsync", set);
    span.setTag("object", nullable(object));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> set.addScoreAsync(object, value));
  }

  @Override
  public RFuture<Integer> addScoreAndGetRevRankAsync(V object, Number value) {
    Span span = tracingHelper.buildSpan("addScoreAndGetRevRankAsync", set);
    span.setTag("object", nullable(object));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> set.addScoreAndGetRevRankAsync(object, value));
  }

  @Override
  public RFuture<Integer> addScoreAndGetRankAsync(V object, Number value) {
    Span span = tracingHelper.buildSpan("addScoreAndGetRankAsync", set);
    span.setTag("object", nullable(object));
    span.setTag("value", nullable(value));
    return tracingHelper.prepareRFuture(span, () -> set.addScoreAndGetRankAsync(object, value));
  }

  @Override
  public RFuture<Collection<V>> valueRangeAsync(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("valueRangeAsync", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper.prepareRFuture(span, () -> set.valueRangeAsync(startIndex, endIndex));
  }

  @Override
  public RFuture<Collection<V>> valueRangeReversedAsync(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("valueRangeReversedAsync", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper
        .prepareRFuture(span, () -> set.valueRangeReversedAsync(startIndex, endIndex));
  }

  @Override
  public RFuture<Collection<ScoredEntry<V>>> entryRangeAsync(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("entryRangeAsync", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper.prepareRFuture(span, () -> set.entryRangeAsync(startIndex, endIndex));
  }

  @Override
  public RFuture<Collection<ScoredEntry<V>>> entryRangeReversedAsync(int startIndex, int endIndex) {
    Span span = tracingHelper.buildSpan("entryRangeReversedAsync", set);
    span.setTag("startIndex", startIndex);
    span.setTag("endIndex", endIndex);
    return tracingHelper
        .prepareRFuture(span, () -> set.entryRangeReversedAsync(startIndex, endIndex));
  }

  @Override
  public RFuture<Collection<V>> valueRangeAsync(double startScore, boolean startScoreInclusive,
      double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("valueRangeAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.prepareRFuture(span,
        () -> set.valueRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public RFuture<Collection<V>> valueRangeReversedAsync(double startScore,
      boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("valueRangeReversedAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.prepareRFuture(span,
        () -> set
            .valueRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public RFuture<Collection<ScoredEntry<V>>> entryRangeAsync(
      double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("entryRangeAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.prepareRFuture(span,
        () -> set.entryRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public RFuture<Collection<V>> valueRangeAsync(double startScore,
      boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset,
      int count) {
    Span span = tracingHelper.buildSpan("valueRangeAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.prepareRFuture(span,
        () -> set
            .valueRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive, offset,
                count));
  }

  @Override
  public RFuture<Collection<V>> valueRangeReversedAsync(double startScore,
      boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset,
      int count) {
    Span span = tracingHelper.buildSpan("valueRangeReversedAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.prepareRFuture(span,
        () -> set
            .valueRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive,
                offset,
                count));
  }

  @Override
  public RFuture<Collection<ScoredEntry<V>>> entryRangeAsync(
      double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive,
      int offset, int count) {
    Span span = tracingHelper.buildSpan("entryRangeAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.prepareRFuture(span,
        () -> set
            .entryRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive, offset,
                count));
  }

  @Override
  public RFuture<Collection<ScoredEntry<V>>> entryRangeReversedAsync(
      double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("entryRangeReversedAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.prepareRFuture(span,
        () -> set
            .entryRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public RFuture<Collection<ScoredEntry<V>>> entryRangeReversedAsync(
      double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive,
      int offset, int count) {
    Span span = tracingHelper.buildSpan("entryRangeReversedAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.prepareRFuture(span,
        () -> set
            .entryRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive,
                offset,
                count));
  }

  @Override
  public RFuture<Integer> countAsync(double startScore, boolean startScoreInclusive,
      double endScore, boolean endScoreInclusive) {
    Span span = tracingHelper.buildSpan("countAsync", set);
    span.setTag("startScore", startScore);
    span.setTag("startScoreInclusive", startScoreInclusive);
    span.setTag("endScore", endScore);
    span.setTag("endScoreInclusive", endScoreInclusive);
    return tracingHelper.prepareRFuture(span,
        () -> set.countAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
  }

  @Override
  public RFuture<Collection<V>> readAllAsync() {
    Span span = tracingHelper.buildSpan("readAllAsync", set);
    return tracingHelper.prepareRFuture(span, set::readAllAsync);
  }

  @Override
  public RFuture<Integer> intersectionAsync(String... names) {
    Span span = tracingHelper.buildSpan("intersectionAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.prepareRFuture(span, () -> set.intersectionAsync(names));
  }

  @Override
  public RFuture<Integer> intersectionAsync(Aggregate aggregate, String... names) {
    Span span = tracingHelper.buildSpan("intersectionAsync", set);
    span.setTag("aggregate", nullable(aggregate));
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.prepareRFuture(span, () -> set.intersectionAsync(aggregate, names));
  }

  @Override
  public RFuture<Integer> intersectionAsync(Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("intersectionAsync", set);
    return tracingHelper.prepareRFuture(span, () -> set.intersectionAsync(nameWithWeight));
  }

  @Override
  public RFuture<Integer> intersectionAsync(Aggregate aggregate,
      Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("intersectionAsync", set);
    span.setTag("aggregate", nullable(aggregate));
    return tracingHelper
        .prepareRFuture(span, () -> set.intersectionAsync(aggregate, nameWithWeight));
  }

  @Override
  public RFuture<Integer> unionAsync(String... names) {
    Span span = tracingHelper.buildSpan("unionAsync", set);
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.prepareRFuture(span, () -> set.unionAsync(names));
  }

  @Override
  public RFuture<Integer> unionAsync(Aggregate aggregate, String... names) {
    Span span = tracingHelper.buildSpan("unionAsync", set);
    span.setTag("aggregate", nullable(aggregate));
    span.setTag("names", Arrays.toString(names));
    return tracingHelper.prepareRFuture(span, () -> set.unionAsync(aggregate, names));
  }

  @Override
  public RFuture<Integer> unionAsync(Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("unionAsync", set);
    return tracingHelper.prepareRFuture(span, () -> set.unionAsync(nameWithWeight));
  }

  @Override
  public RFuture<Integer> unionAsync(Aggregate aggregate, Map<String, Double> nameWithWeight) {
    Span span = tracingHelper.buildSpan("unionAsync", set);
    span.setTag("aggregate", nullable(aggregate));
    return tracingHelper.prepareRFuture(span, () -> set.unionAsync(aggregate, nameWithWeight));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAsync", set);
    span.setTag("order", nullable(order));
    return tracingHelper.prepareRFuture(span, () -> set.readSortAsync(order));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAsync", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.prepareRFuture(span, () -> set.readSortAsync(order, offset, count));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(String byPattern, SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingHelper.prepareRFuture(span, () -> set.readSortAsync(byPattern, order));
  }

  @Override
  public RFuture<Set<V>> readSortAsync(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(span, () -> set.readSortAsync(byPattern, order, offset, count));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAsync(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingHelper
        .prepareRFuture(span, () -> set.readSortAsync(byPattern, getPatterns, order));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAsync(String byPattern, List<String> getPatterns,
      SortOrder order,
      int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(span, () -> set.readSortAsync(byPattern, getPatterns, order, offset, count)
        );
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("order", nullable(order));
    return tracingHelper.prepareRFuture(span, () -> set.readSortAlphaAsync(order));
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(order, offset, count));
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(String byPattern, SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(byPattern, order));
  }

  @Override
  public RFuture<Set<V>> readSortAlphaAsync(String byPattern, SortOrder order, int offset,
      int count) {
    Span span = tracingHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(byPattern, order, offset, count));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingHelper
        .prepareRFuture(span, () -> set.readSortAlphaAsync(byPattern, getPatterns, order));
  }

  @Override
  public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns,
      SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAlphaAsync", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(span,
            () -> set.readSortAlphaAsync(byPattern, getPatterns, order, offset, count)
        );
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, SortOrder order) {
    Span span = tracingHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    return tracingHelper.prepareRFuture(span, () -> set.sortToAsync(destName, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName,
      SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, order, offset, count));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      SortOrder order) {
    Span span = tracingHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, byPattern, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, byPattern, order, offset, count));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      List<String> getPatterns, SortOrder order) {
    Span span = tracingHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    return tracingHelper
        .prepareRFuture(span, () -> set.sortToAsync(destName, byPattern, getPatterns, order));
  }

  @Override
  public RFuture<Integer> sortToAsync(String destName, String byPattern,
      List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("sortToAsync", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .prepareRFuture(
            span, () -> set.sortToAsync(destName, byPattern, getPatterns, order, offset, count));
  }

  @Override
  public Iterator<V> iterator() {
    return set.iterator();
  }

  @Override
  public void forEach(Consumer<? super V> action) {
    Span span = tracingHelper.buildSpan("forEach", set);
    span.setTag("action", nullable(action));
    tracingHelper.decorate(span, () -> set.forEach(action));
  }

  @Override
  public Spliterator<V> spliterator() {
    return set.spliterator();
  }

  @Override
  public Set<V> readSort(SortOrder order) {
    Span span = tracingHelper.buildSpan("readSort", set);
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.readSort(order));
  }

  @Override
  public Set<V> readSort(SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSort", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> set.readSort(order, offset, count));
  }

  @Override
  public Set<V> readSort(String byPattern, SortOrder order) {
    Span span = tracingHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.readSort(byPattern, order));
  }

  @Override
  public Set<V> readSort(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> set.readSort(byPattern, order, offset, count));
  }

  @Override
  public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order) {
    Span span = tracingHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.readSort(byPattern, getPatterns, order));
  }

  @Override
  public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order,
      int offset, int count) {
    Span span = tracingHelper.buildSpan("readSort", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .decorate(span, () -> set.readSort(byPattern, getPatterns, order, offset, count));
  }

  @Override
  public Set<V> readSortAlpha(SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAlpha", set);
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.readSortAlpha(order));
  }

  @Override
  public Set<V> readSortAlpha(SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAlpha", set);
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> set.readSortAlpha(order, offset, count));
  }

  @Override
  public Set<V> readSortAlpha(String byPattern, SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.readSortAlpha(byPattern, order));
  }

  @Override
  public Set<V> readSortAlpha(String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> set.readSortAlpha(byPattern, order, offset, count));
  }

  @Override
  public <T> Collection<T> readSortAlpha(String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.readSortAlpha(byPattern, getPatterns, order));
  }

  @Override
  public <T> Collection<T> readSortAlpha(String byPattern,
      List<String> getPatterns, SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("readSortAlpha", set);
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .decorate(span, () -> set.readSortAlpha(byPattern, getPatterns, order, offset, count));
  }

  @Override
  public int sortTo(String destName, SortOrder order) {
    Span span = tracingHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.sortTo(destName, order));
  }

  @Override
  public int sortTo(String destName, SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper.decorate(span, () -> set.sortTo(destName, order, offset, count));
  }

  @Override
  public int sortTo(String destName, String byPattern, SortOrder order) {
    Span span = tracingHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.sortTo(destName, byPattern, order));
  }

  @Override
  public int sortTo(String destName, String byPattern, SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .decorate(span, () -> set.sortTo(destName, byPattern, order, offset, count));
  }

  @Override
  public int sortTo(String destName, String byPattern, List<String> getPatterns,
      SortOrder order) {
    Span span = tracingHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    return tracingHelper.decorate(span, () -> set.sortTo(destName, byPattern, getPatterns, order));
  }

  @Override
  public int sortTo(String destName, String byPattern, List<String> getPatterns,
      SortOrder order, int offset, int count) {
    Span span = tracingHelper.buildSpan("sortTo", set);
    span.setTag("destName", nullable(destName));
    span.setTag("byPattern", nullable(byPattern));
    span.setTag("getPatterns", collectionToString(getPatterns));
    span.setTag("order", nullable(order));
    span.setTag("offset", offset);
    span.setTag("count", count);
    return tracingHelper
        .decorate(span, () -> set.sortTo(destName, byPattern, getPatterns, order, offset, count));
  }

}
