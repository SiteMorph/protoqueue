package net.sitemorph.queue;

import static org.joda.time.DateTime.now;

import net.sitemorph.protostore.CrudException;
import net.sitemorph.protostore.CrudIterator;
import net.sitemorph.protostore.CrudStore;
import net.sitemorph.protostore.DbUrnFieldStore;
import net.sitemorph.protostore.MessageNotFoundException;
import net.sitemorph.protostore.MessageVectorException;
import net.sitemorph.protostore.SortOrder;
import net.sitemorph.queue.Message.Task;

import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

/**
 * The task queue builder constructs a task queue from a set of configuration
 * including a crud store. This class is simply a wrapper for a crud store with
 * convenience builder which only requires a connection and table name.
 *
 * @author damien@sitemorph.net
 */
public class CrudTaskQueue implements TaskQueue {

  private CrudStore<Task> taskStore;
  private Connection connection;
  private Logger log = LoggerFactory.getLogger(getClass());
  private TaskPicker picker;

  private CrudTaskQueue(CrudStore<Task> taskStore, Connection connection,
      TaskPicker picker) {
    this.taskStore = taskStore;
    this.connection = connection;
    this.picker = picker;
  }

  private CrudTaskQueue(CrudStore<Task> taskStore, TaskPicker picker) {
    this.taskStore = taskStore;
    this.picker = picker;
  }

  /**
   * Helper method that uses the default task picker.
   * @param taskStore to build from.
   * @return the crud task queue.
   */
  public static CrudTaskQueue fromCrudStore(CrudStore<Task> taskStore) {
    // TODO(dka) Consider refactoring into a factory the default picker
    return new CrudTaskQueue(taskStore, new NaiveTaskPicker());
  }

  public static CrudTaskQueue fromCrudStore(CrudStore<Task> taskStore,
      TaskPicker picker) {
    return new CrudTaskQueue(taskStore, picker);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * @see TaskQueue#claim(UUID, long, long)
   */
  @Override
  public Task claim(UUID identity, long now, long claimTimeout) throws QueueException {
    try {
      Task run = picker.pick(now, taskStore);
      if (null == run) {
        return null;
      }
      Task result = null;
      try {
        result = taskStore.update(run.toBuilder()
            .setClaim(identity.toString())
            .setClaimTimeout(claimTimeout));
        log.debug("{} successfully claimed task          {}",
            identity, result.getUrn());
      } catch (MessageVectorException e) {
        log.debug("{} attempted claim of task             {} failed due to contention. " +
            "Deferring", identity, run);
        result = null;
      } catch (MessageNotFoundException e) {
        log.debug("{} attempted claim of task             {} failed due to task already gone." +
            "Continuing", identity, run);
        result = null;
      }
      return result;
    } catch (MessageVectorException e) {
      throw new StaleClaimException("Claim attempted when already claimed.", e);
    } catch (CrudException e) {
      throw new QueueException("Storage error claiming from queue", e);
    }
  }

  /**
   * @see TaskQueue#release(Task)
   */
  public void release(Task task) throws QueueException {
    try {
      taskStore.update(task.toBuilder()
          .clearClaim()
          .clearClaimTimeout());
    } catch (MessageVectorException e) {
      throw new StaleClaimException("release attempt when claim out of date", e);
    } catch (MessageNotFoundException e) {
      throw new StaleClaimException("Storage error releasing task", e);
    } catch (CrudException e) {
      throw new QueueException("Queue Storage Error", e);
    }
  }

  /**
   * @see TaskQueue#push(Task.Builder)
   */
  @Override
  public Task push(Task.Builder task) throws QueueException {
    try {
      if (!task.hasRunTime()) {
        task.setRunTime(now(DateTimeZone.UTC).getMillis());
      }
      return taskStore.create(task);
    } catch (CrudException e) {
      throw new QueueException("Error adding task to queue", e);
    }
  }

  /**
   * @see TaskQueue#remove(Task)
   */
  @Override
  public void remove(Task task) throws QueueException {
    try {
      taskStore.delete(task);
    } catch (CrudException e) {
      throw new QueueException("Error removing task", e);
    }
  }

  /**
   * @see TaskQueue#tasks()
   */
  @Override
  public CrudIterator<Task> tasks() throws QueueException {
    try {
      return taskStore.read(Task.newBuilder());
    } catch (CrudException e) {
      throw new QueueException("Error getting task list", e);
    }
  }

  /**
   * @see TaskQueue#close()
   */
  @Override
  public void close() throws QueueException {
    try {
      taskStore.close();
    } catch (CrudException e) {
      throw new QueueException("Error closing task queue", e);
    }
    // also close the underlying connection if it is open.
    try {
      if (null != connection && !connection.isClosed()) {
        connection.close();
        connection = null;
      }
    } catch (SQLException e) {
      throw new QueueException("Error closing SQL connection", e);
    }
  }

  /**
   * Build a crud task queue using concrete urn field store implementation.
   */
  public static class Builder {

    private Connection connection;
    private TaskPicker picker = null;

    private Builder() {
      taskStore = new DbUrnFieldStore.Builder<Task>();
    }

    private DbUrnFieldStore.Builder<Task> taskStore;

    /**
     * Set the name of the store table.
     *
     * @param tableName of the underlying table.
     * @return builder
     */
    public Builder setTableName(String tableName) {
      taskStore.setTableName(tableName);
      return this;
    }

    /**
     * Set the underlying SQL connector
     * @param connection to use to access the queue
     * @return builder
     */
    public Builder setConnection(Connection connection) {
      taskStore.setConnection(connection);
      this.connection = connection;
      return this;
    }

    /**
     * Override the default naive task picker.
     *
     * @param picker new task picker
     */
    public Builder setTaskPicker(TaskPicker picker) {
      this.picker = picker;
      return this;
    }

    /**
     * construct teh task queue.
     * @return the task queue
     * @throws QueueException on connector error or table and field name error
     */
    public CrudTaskQueue build() throws QueueException {
      try {
        taskStore
            .setPrototype(Task.newBuilder())
            .setUrnColumn("urn")
            .addIndexField("path")
            .setSortOrder("runTime", SortOrder.ASCENDING)
            .setVectorField("vector");
        CrudStore<Task> store = taskStore.build();
        if (null == picker) {
          picker = new NaiveTaskPicker();
        }
        return new CrudTaskQueue(store, connection, picker);
      } catch (CrudException e) {
        throw new QueueException("Error initialising queue", e);
      }
    }
  }
}
