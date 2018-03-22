package com.akolb;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static com.akolb.Util.filterMatches;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

class UtilTest {

  @Test
  void filterMatchesEmpty() {
    List<String> candidates = ImmutableList.of("a", "b");
    assertThat(filterMatches(candidates, null), is(candidates));
    assertThat(filterMatches(null, candidates), is(Collections.emptyList()));
  }

  @Test
  void filterMatchesPositive() {
    List<String> candidates = ImmutableList.of("a", "b");
    List<String> expected = ImmutableList.of("a");
    List<String> filtered = filterMatches(candidates, Collections.singletonList("a"));
    assertThat(filtered, is(expected));
  }

  @Test
  void filterMatchesNegative() {
    List<String> candidates = ImmutableList.of("a", "b");
    List<String> expected = ImmutableList.of("a");
    assertThat(filterMatches(candidates, Collections.singletonList("!b")), is(expected));
  }

  @Test
  void filterMatchesMultiple() {
    List<String> candidates = ImmutableList.of("a", "b", "any", "boom", "hello");
    List<String> patterns = ImmutableList.of("^a", "!y$");
    List<String> expected = ImmutableList.of("a");
    assertThat(filterMatches(candidates, patterns), is(expected));
  }


}