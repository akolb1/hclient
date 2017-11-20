package com.akolb;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Run a set of benchmarks as a suite.
 * Every benchmark has an associated name. Caller can either run all benchmarks
 * or only ones matching the filter.
 */
public class BenchmarkSuite {
  // Collection of benchmarks
  private final Map<String, Supplier<DescriptiveStatistics>> suite = new TreeMap<>();

  public static Map<String, DescriptiveStatistics> runAll(@Nonnull Map<String,
      Supplier<DescriptiveStatistics>> suite) {
    Map<String, DescriptiveStatistics> result = new TreeMap<>();
    suite.forEach((k, v) -> result.put(k, v.get()));
    return result;
  }

  public static Map<String, DescriptiveStatistics> runMatching(@Nonnull Map<String,
      Supplier<DescriptiveStatistics>> suite, @Nonnull List<String> patterns) {
    Map<String, DescriptiveStatistics> result = new TreeMap<>();
    suite.keySet()
        .stream()
        .filter(s -> matches(s, patterns))
        .forEach(k -> result.put(k, suite.get(k).get()));
    return result;
  }

  public @Nonnull Map<String, DescriptiveStatistics> runAll() {
    return runAll(suite);
  }

  public @Nonnull Map<String, DescriptiveStatistics> runMatching(List<String> patterns) {
    return patterns == null || patterns.isEmpty() ?
        runAll(suite) :
        runMatching(suite, patterns);
  }

  public void add(@Nonnull String name, @Nonnull Supplier<DescriptiveStatistics> b) {
    suite.put(name, b);
  }

  private static boolean matches(@Nonnull String what, @Nonnull List<String> patterns) {
    return patterns.stream().anyMatch(what::matches);
  }
}
