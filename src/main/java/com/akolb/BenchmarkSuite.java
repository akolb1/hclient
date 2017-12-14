/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.akolb;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Run a set of benchmarks as a suite.
 * Every benchmark has an associated name. Caller can either run all benchmarks
 * or only ones matching the filter.
 */
public class BenchmarkSuite {
  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkSuite.class);
  // Delta margin for data sanitizing
  private final static double MARGIN = 2;
  // Collection of benchmarks
  private final Map<String, Supplier<DescriptiveStatistics>> suite = new HashMap<>();
  // List of benchmarks. All benchmarks are executed in the order
  // they are inserted
  private final List<String> benchmarks = new ArrayList<>();
  private final Map<String, DescriptiveStatistics> result = new TreeMap<>();
  private final boolean doSanitize;
  private long scale = 1;
  private double minMean = 0;

  /**
   * Create new benchmark suite without data sanitizing
   */
  public BenchmarkSuite() {
    this(false);
  }

  BenchmarkSuite setScale(long scale) {
    this.scale = scale;
    return this;
  }

  /**
   * Create new benchmark suite.<p>
   * <p>
   * The suite can run multiple benchmarks.
   *
   * @param doSanitize Sanitize data if true
   */
  BenchmarkSuite(boolean doSanitize) {
    this.doSanitize = doSanitize;
  }

  public Map<String, DescriptiveStatistics> getResult() {
    return result;
  }

  public List<String> list(@Nullable List<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return benchmarks;
    }
    return benchmarks
        .stream()
        .filter(s -> matches(s, patterns))
        .collect(Collectors.toList());
  }

  private BenchmarkSuite runAll() {
    if (doSanitize) {
      benchmarks.forEach(name -> {
        LOG.info("Running benchmark {}", name);
        result.put(name, sanitize(suite.get(name).get()));
      });
    } else {
      benchmarks.forEach(name -> {
        LOG.info("Running benchmark {}", name);
        result.put(name, suite.get(name).get());
      });
    }
    return this;
  }

  public BenchmarkSuite runMatching(@Nullable List<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return runAll();
    }
    if (doSanitize) {
      benchmarks
          .stream()
          .filter(s -> matches(s, patterns))
          .forEach(k -> {
            LOG.info("Running benchmark {}", k);
            result.put(k, sanitize(suite.get(k).get()));
          });
    } else {
      benchmarks
          .stream()
          .filter(s -> matches(s, patterns))
          .forEach(k -> {
            LOG.info("Running benchmark {}", k);
            result.put(k, suite.get(k).get());
          });
    }
    return this;
  }

  public BenchmarkSuite add(@NotNull String name, @NotNull Supplier<DescriptiveStatistics> b) {
    suite.put(name, b);
    benchmarks.add(name);
    return this;
  }

  private static boolean matches(@NotNull String what, @NotNull List<String> patterns) {
    return patterns.stream().anyMatch(what::matches);
  }

  /**
   * Get new statistics that excludes values beyond mean +/- 2 * stdev
   *
   * @param data Source data
   * @return new {@link @DescriptiveStatistics objects with sanitized data}
   */
  private static DescriptiveStatistics sanitize(DescriptiveStatistics data) {
    double meanValue = data.getMean();
    double delta = MARGIN * meanValue;
    double minVal = meanValue - delta;
    double maxVal = meanValue + delta;
    return new DescriptiveStatistics(Arrays.stream(data.getValues())
        .filter(x -> x > minVal && x < maxVal)
        .toArray());
  }

  private static double median(DescriptiveStatistics data) {
    return new Median().evaluate(data.getValues());
  }

  /**
   * @return minimum of all mean values
   */
  private double minMean() {
    double[] data = result.entrySet()
        .stream()
        .mapToDouble(e -> e.getValue().getMean())
        .toArray();
    return new DescriptiveStatistics(data).getMin();
  }

  private void displayStats(Formatter fmt, String name, DescriptiveStatistics stats) {
    double mean = stats.getMean();
    double err = stats.getStandardDeviation() / mean * 100;

    fmt.format("%-30s %-6.3g %-6.3g %-6.3g %-6.3g %-6.3g %-6.3g%n",
        name,
        (mean - minMean) / scale,
        mean / scale,
        median(stats) / scale,
        stats.getMin() / scale,
        stats.getMax() / scale,
        err);
  }

  private void displayCSV(Formatter fmt, String name, DescriptiveStatistics stats, String separator) {
    double mean = stats.getMean();
    double err = stats.getStandardDeviation() / mean * 100;

    fmt.format("%s%s%g%s%g%s%g%s%g%s%g%s%g%n",
        name, separator,
        (mean - minMean) / scale, separator,
        mean / scale, separator,
        median(stats) / scale, separator,
        stats.getMin() / scale, separator,
        stats.getMax() / scale, separator,
        err);
  }

  BenchmarkSuite display(Formatter fmt) {
    fmt.format("%-30s %-6s %-6s %-6s %-6s %-6s %-6s%n",
        "Operation", "AMean", "Mean", "Med", "Min", "Max", "Err%");
    minMean = minMean();
    result.forEach((name, stat) -> displayStats(fmt, name, stat));
    return this;
  }

  BenchmarkSuite displayCSV(Formatter fmt, String separator) {
    fmt.format("%s%s%s%s%6s%s%s%s%s%s%s%s%s%n",
        "Operation", separator, "AMean", separator, "Mean", separator, "Med", separator, "Min",
        separator, "Max", separator, "Err%");
    minMean = minMean();
    result.forEach((name, s) -> displayCSV(fmt, name, s, separator));
    return this;
  }
}
