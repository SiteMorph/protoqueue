package net.sitemorph.queue;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.queue.Message.Task;
import net.sitemorph.queue.Message.Task.Builder;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * A list backed queue. Always pops from position 0. Pushes to n
 *
 * @author damien@sitemorph.net
 */
public class ListTaskQueue implements TaskQueue {

  private final List<Task> taskList;

  public ListTaskQueue(List<Task> taskList) {
    this.taskList = taskList;
  }

  @Override
  public Task peek() throws QueueException {
    if (taskList.isEmpty()) {
      return null;
    }
    return taskList.get(0);
  }

  @Override
  public Task pop() throws QueueException {
    if (taskList.isEmpty()) {
      return null;
    }
    return taskList.remove(0);
  }

  @Override
  public Task push(Builder task) throws QueueException {
    Task result = task.setUrn(UUID.randomUUID().toString())
        .build();
    taskList.add(result);
    return result;
  }

  @Override
  public void remove(Task task) throws QueueException {
    taskList.remove(task);
  }

  @Override
  public CrudIterator<Task> tasks() throws QueueException {
    final Iterator<Task> taskIterator = taskList.iterator();
    return new CrudIterator<Task>() {
      @Override
      public Task next() throws CrudException {
        return taskIterator.next();
      }

      @Override
      public boolean hasNext() throws CrudException {
        return taskIterator.hasNext();
      }

      @Override
      public void close() throws CrudException {}
    };
  }

  @Override
  public void close() throws QueueException {
    taskList.clear();
  }
}
