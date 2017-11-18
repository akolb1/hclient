package com.akolb;

import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Micro-benchmark some piece of code.<p>
 * 
 * Every benchmark has three parts:
 * <ul>
 *   <li>Optional pre-test</li>
 *   <li>Mandatory test</lI>
 *   <li>Optional post-test</li>
 * </ul>
 * Measurement consists of the warm-up phase and measurement phase. Consumer can specify
 * number of times the warmup and measurement is repeated.<p>
 * All time is measured in nanoseconds.
 */
public class MicroBenchmark {
  private static final int WARMUP_DEFAULT = 15;
  private static final int ITERATIONS_DEFAULT = 100;
  
  private final int warmup;
  private final int iterations;

  /**
   * Create default micro benchmark measurer
   */
  public MicroBenchmark() {
    this(WARMUP_DEFAULT, ITERATIONS_DEFAULT);
  }

  /**
   * Create micro benchmark measurer.
   * @param warmup number of test calls for warmup
   * @param iterations number of test calls for measurement
   */
  public MicroBenchmark(int warmup, int iterations) {
    this.warmup = warmup;
    this.iterations = iterations;
  }

  /**
   * Run the benchmark and measure run-time statistics in nanoseconds.<p>
   * Before the run the warm-up phase is executed.
   * @param pre Optional pre-test setup
   * @param test Mandatory test
   * @param post Optional post-test cleanup
   * @return Statistics describing the results. All times are in nanoseconds.
   */
  public DescriptiveStatistics measure(@Nullable Runnable pre,
                                       @Nonnull Runnable test,
                                       @Nullable Runnable post) {
    for (int i = 0; i < warmup; i++) {
      if (pre != null) {
        pre.run();
      }
      test.run();
      if (post != null) {
        post.run();
      }
    }
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (int i = 0; i < iterations; i++) {
      if (pre != null) {
        pre.run();
      }
      long start = System.nanoTime();
      test.run();
      long end = System.nanoTime();
      stats.addValue((double)(end - start));
      if (post != null) {
        post.run();
      }
    }
    return stats;
  }

  /**
   * Run the benchmark and measure run-time statistics in nanoseconds.<p>
   * Before the run the warm-up phase is executed. No pre or post operations are executed.
   * @param test test to measure
   * @return Statistics describing the results. All times are in nanoseconds.
   */
  public DescriptiveStatistics measure(@NonNull Runnable test) {
    return measure(null, test, null);
  }
}
