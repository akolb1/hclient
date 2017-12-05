package com.akolb;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
  // Collection of benchmarks
  private final Map<String, Supplier<DescriptiveStatistics>> suite = new HashMap<>();
  // List of benchmarks. All benchmarks are executed in the order
  // they are inserted
  private final List<String> benchmarks = new ArrayList<>();

  public List<String> list(@Nullable List<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return benchmarks;
    }
    return benchmarks
        .stream()
        .filter(s -> matches(s, patterns))
        .collect(Collectors.toList());
  }

  private Map<String, DescriptiveStatistics> runAll() {
    Map<String, DescriptiveStatistics> result = new TreeMap<>();
    benchmarks.forEach(name -> {
      LOG.info("Running benchmark {}", name);
      result.put(name, suite.get(name).get());
    });
    return result;
  }

  public Map<String, DescriptiveStatistics> runMatching(@Nullable List<String> patterns) {
    if (patterns == null || patterns.isEmpty()) {
      return runAll();
    }
    Map<String, DescriptiveStatistics> result = new TreeMap<>();
    benchmarks
        .stream()
        .filter(s -> matches(s, patterns))
        .forEach(k -> {
          LOG.info("Running benchmark {}", k);
          result.put(k, suite.get(k).get());
        });
    return result;
  }

  public BenchmarkSuite add(@NotNull String name, @NotNull Supplier<DescriptiveStatistics> b) {
    suite.put(name, b);
    benchmarks.add(name);
    return this;
  }

  private static boolean matches(@NotNull String what, @NotNull List<String> patterns) {
    return patterns.stream().anyMatch(what::matches);
  }
}
