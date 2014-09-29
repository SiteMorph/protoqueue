package net.sitemorph.queue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.MessageVectorException;
import net.sitemorph.queue.Message.Task;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.joda.time.DateTime.now;

/**
 * The naive task picker provides a simple reference implementation of the task
 * picker. It doesn't deal with fairness or other concerns - primarily looking
 * at scheuled time ordering.
 *
 * @author damien@sitemorph.net
 */
public class NaieveTaskPicker implements TaskPicker {

  private Logger log = LoggerFactory.getLogger(getClass());

  @Override
  public Task pick(long now, CrudStore<Task> taskStore) throws QueueException {
    Task claim = null;
    Task reclaim = null;
    try {
      CrudIterator<Task> tasks = taskStore.read(Task.newBuilder());
      while (tasks.hasNext()) {
        Task candidate = tasks.next();
        // If the task is in the future then return
        if (isFutureTask(candidate)) {
          break;
        }

        if (candidate.hasClaim()) {
          if (now > candidate.getClaimTimeout() && null == reclaim) {
            reclaim = candidate;
          }
        } else {
          claim = candidate;
        }
      }
      tasks.close();

      Task result = null;
      if (null == claim) {
        if (null != reclaim) {
          result = reclaim;
        }
      } else {
        result = claim;
      }
      return result;
    } catch (MessageVectorException e) {
      throw new QueueException("Claim attempted when already claimed.", e);
    } catch (CrudException e) {
      throw new QueueException("Storage error claiming from queue", e);
    }
  }

  private boolean isFutureTask(Task task) {
    return now(DateTimeZone.UTC).isBefore(task.getRunTime());
  }
}
