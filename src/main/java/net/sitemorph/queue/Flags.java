package net.sitemorph.queue;

/**
 * Interface for flag providers to use in order to signal different status
 * information to the dispatcher.
 *
 * @author damien@sitemorph.net
 */
public interface Flags {

  public boolean isPaused();
}
