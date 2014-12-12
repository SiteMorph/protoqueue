package net.sitemorph.queue;

import static org.testng.Assert.assertTrue;

import org.joda.time.Interval;
import org.testng.annotations.Test;

/**
 * Test Interval assumptions.
 *
 * @author damien@sitemorph.net
 */
public class IntervalTest {

  @Test
  public void testAfterInterval() {
    long now = System.currentTimeMillis();
    Interval interval = new Interval(now, now + 10);
    assertTrue(interval.isBefore(now + 11));
  }
}
