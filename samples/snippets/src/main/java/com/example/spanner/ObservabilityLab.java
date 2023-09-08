package com.example.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class helps in the execution of observability lab(lab3), it basically shows the usage of
 * request tags, transaction tags, hotspot etc. Method written and execute for the duration to
 * simulate these behaviour.
 */
public class ObservabilityLab {

  private static final String LOREM_IPSUM =
      "Aenean sit amet lectus est. Nullam ornare ligula luctus auctor placerat. Morbi fermentum volutpat massa, sit amet consectetur metus vehicula sed. Sed auctor scelerisque tempus. Morbi hendrerit tortor et felis scelerisque, at fermentum risus posuere. Suspendisse finibus, tellus eu ullamcorper posuere, odio ex pulvinar est, a malesuada libero turpis quis massa. Quisque placerat lacus orci. Proin ac viverra metus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus cursus rhoncus iaculis. Pellentesque nisl tellus, venenatis nec erat sit amet, eleifend porttitor nisl. Maecenas fringilla ex id mauris facilisis, sed luctus dui elementum. Suspendisse ac orci lectus. Suspendisse vitae sapien convallis, commodo leo ut, ultricies arcu. Fusce pellentesque sem vestibulum, sodales purus eget, auctor odio. Ut et nunc metus. Aenean ac ex faucibus, tristique nibh ut, euismod lorem. Fusce a ex ut nibh consectetur mollis. Aenean suscipit elit dui, faucibus vestibulum leo commodo a. Nulla ultricies vitae velit cursus commodo. Morbi eu sapien in magna condimentum porta quis eget sem. Etiam tempor auctor diam, quis mollis odio scelerisque et. Fusce tempus mauris mi, et varius enim condimentum in. Aliquam nisi lorem, pulvinar id ullamcorper vitae, fringilla vel leo. Fusce vehicula tincidunt vulputate. Vivamus efficitur nunc quis lorem porttitor elementum. Donec ex neque, vestibulum nec mollis quis, lacinia quis dui. Nullam rhoncus quis lacus nec euismod. Vivamus porttitor sem nec nisl auctor ultrices. Vivamus non laoreet lectus. Aliquam condimentum semper libero eu elementum. Nullam lobortis ultricies gravida. Integer in lacinia lacus, ac consequat magna. Suspendisse et risus vel diam facilisis ornare a in arcu. Nulla nec nunc sem. Cras aliquam nulla sem, luctus maximus est gravida ut. Ut pellentesque pharetra convallis. Quisque molestie, ipsum sit amet scelerisque convallis, magna ante fringilla massa, in blandit turpis nibh ornare magna. Curabitur mi tortor, feugiat id sem ac, scelerisque congue tortor. Aenean non viverra risus. Praesent vel enim quis dolor auctor aliquet. Maecenas faucibus mi at venenatis suscipit. Integer interdum magna vitae mauris interdum, laoreet ullamcorper erat tincidunt. Morbi vel ipsum convallis, semper quam vitae, sagittis tellus. In facilisis eu lorem imperdiet laoreet. Suspendisse gravida a magna et condimentum. Suspendisse vitae risus vitae est pulvinar convallis at sit amet sem. Morbi vel imperdiet leo, sit amet cursus urna. Pellentesque suscipit ut neque non laoreet. Duis non ipsum ipsum. Quisque ut porttitor dui. Duis nulla augue, varius quis tellus sed, efficitur bibendum justo. Cras vestibulum congue ante in gravida. Quisque tincidunt nisi nisl, sed vulputate leo luctus ac. Pellentesque quis tempor leo, sed faucibus ipsum. Donec rutrum turpis nec auctor lobortis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Sed lobortis sodales risus vitae eleifend. Nullam pharetra dapibus lobortis. Vestibulum nisl diam, dignissim sit amet lacus sit amet, venenatis auctor nisi. Phasellus sollicitudin tortor a mi fringilla, imperdiet tristique ex feugiat. Nulla ut vehicula metus, nec tempus odio. Phasellus pellentesque lacus lacus, fringilla egestas felis hendrerit ac. Nam suscipit orci eros, at molestie nunc tincidunt id. Aliquam maximus finibus leo. Vestibulum orci turpis, fringilla eget lorem id, cursus consequat tortor. Suspendisse efficitur purus lectus, ac ornare sem lobortis a. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed sodales id mi eu aliquam. Curabitur enim libero, tempor vel vestibulum id, eleifend at lacus. Maecenas ut feugiat justo, nec dictum justo. Nulla porttitor accumsan rhoncus. Cras ligula velit, molestie sit amet molestie sed, commodo et libero. Etiam eu condimentum arcu. Aliquam erat volutpat. Nullam sit amet urna ipsum. Vestibulum at velit id velit tincidunt ultricies a ut metus. In vitae mollis diam. Proin lacinia fringilla purus vitae aliquet. Vestibulum a tincidunt eros. Aenean imperdiet aliquet arcu, vitae euismod turpis fermentum quis. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Suspendisse potenti. Pellentesque quis efficitur mauris, pulvinar sagittis ante. Cras efficitur porta convallis. Nullam eleifend congue finibus. Nam sodales tincidunt odio ac elementum. Sed sem risus, imperdiet quis suscipit eu, imperdiet vitae nibh. Morbi gravida neque ac sodales varius. In convallis massa vel lectus fermentum ultricies ac vitae eros. Sed justo sem, dignissim sit amet tempus ultrices, pellentesque at libero. Quisque a quam volutpat, tristique erat a, auctor enim. Suspendisse finibus arcu erat, nec mattis tortor facilisis in. Donec ornare mattis lorem, in interdum turpis venenatis vel. Mauris tempus porttitor libero. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vivamus eget tempor dui. Nunc et elit eu eros tincidunt sapien.";
  private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();
  private final AtomicLong msdId = new AtomicLong(1);

  /**
   * This method creates Message table which will be used for monotonically increasing writes.
   *
   * @param dbAdminClient DatabaseAdminClient
   * @param id Database object
   */
  public void createMessages(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Message("
                + " msg_id STRING(MAX) NOT NULL,"
                + " body STRING(MAX),"
                + ") PRIMARY KEY (msg_id)"));
  }

  /** Calling the method for parallel execution */
  public void performMonotonicallyIncreasingWritesParallel(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> performMonotonicallyIncreasingWrite(dbClient, mutationsPerTransaction, numMinutes),
        NUM_PROCESSORS);
  }

  /**
   * Calling the method for parallel execution
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numMinutes How long this method should execute in minutes.
   */
  public void performMultiParticipantWriteParallel(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> performMultiParticipantWrite(dbClient, mutationsPerTransaction, numMinutes),
        NUM_PROCESSORS);
  }

  /**
   * This method creates Mailbox and Message table which are interleaved.
   *
   * @param dbAdminClient DatabaseAdminClient
   * @param id Database object
   */
  public void createInterleaved(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Mailbox("
                + "sid INT64 NOT NULL,"
                + "state STRING(MAX),"
                + ") PRIMARY KEY (sid)",
            "CREATE TABLE Message("
                + "sid INT64 NOT NULL,"
                + "msg_id STRING(MAX) NOT NULL,"
                + "body STRING(MAX),"
                + "send_timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true),"
                + ") PRIMARY KEY (sid, msg_id),"
                + "INTERLEAVE IN PARENT Mailbox ON DELETE CASCADE"));
  }

  /** Writing the value in the Mailbox table. */
  public void writeMailboxes(DatabaseClient dbClient, int numMailboxes) {
    List<Mutation> mutations = new ArrayList<>();
    for (int sid = 0; sid < numMailboxes; ++sid) {
      mutations.add(Mutation.newInsertBuilder("Mailbox").set("sid").to(sid).build());
    }
    try {
      dbClient.writeWithOptions(
          mutations, Options.tag("app=lab3,env=dev,case=writeMailBox,action=insert"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Parallel writing the values in the Messages table. Here Message table is interleaved in
   * Mailbox.
   *
   * @param numMailboxes number of Mailbox to be used to write values in interleaves Message table.
   * @param mutationsPerTransaction number of Mutations per transaction
   * @param numMinutes how long it should write.
   */
  public void writeMessagesInterleavedParallel(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> writeMessagesInterleaved(dbClient, numMailboxes, mutationsPerTransaction, numMinutes),
        NUM_PROCESSORS);
  }

  /**
   * This method creates the WorkList table.
   *
   * @param dbAdminClient DatabaseAdminClient
   * @param id Database object
   */
  public void createWorkItems(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE WorkList("
                + " id STRING(MAX) NOT NULL,"
                + " is_done BOOL,"
                + " timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true), "
                + ") PRIMARY KEY (id)"));
  }

  /**
   * This method inserts data in the worklist table which can also be called as work items
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numWorkItems Number of work items to be written in the table. This is basically the
   *     number of rows should be inserted in the worklist table.
   */
  public void writeWorkItems(
      DatabaseClient dbClient, int numWorkItems, int mutationsPerTransaction) {
    // Finding the number of transaction to run the outer loop
    int numTransactions = numWorkItems / mutationsPerTransaction;

    for (int i = 0; i < numTransactions; ++i) {
      List<Mutation> mutations = new ArrayList<>();

      for (int j = 0; j < mutationsPerTransaction; ++j) {
        mutations.add(
            Mutation.newInsertBuilder("WorkList")
                .set("id")
                .to(UUID.randomUUID().toString())
                .set("is_done")
                .to(false)
                .set("timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
      }
      try {
        dbClient.writeWithOptions(
            mutations, Options.tag("app=lab3,env=dev,case=WorkListItem,action=insert"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Calling the method for parallel execution */
  public void doWorkSingleTransactionParallel(DatabaseClient dbClient, boolean isLocking) {
    executeTasksInParallel(
        () -> doWorkSingleTransactionSerial(dbClient, isLocking), NUM_PROCESSORS);
  }

  /**
   * This method writes the data into the Message table. Here primary key will be monotonically
   * increasing.
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numMinutes How long this method should execute in minutes.
   */
  private void performMonotonicallyIncreasingWrite(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                // This will add the incremental value and be thread safe. While re-running need to
                // make sure that we delete the Ids from Table  or create fresh database as this
                // will always start from 1
                .to(msdId.getAndIncrement())
                .set("body")
                .to(LOREM_IPSUM)
                .build());
      }
      try {
        dbClient.writeWithOptions(
            mutations,
            // Tag has a limit of 50 characters hence keeping the tag shorts
            Options.tag("app=lab3,env=dev,case=incremental,action=insert"));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Below method inserts the data in the table using random UUID value of primary. This method is
   * expected to generate a mutli participant write in the table. Initial few minutes are required
   * for warm up after which spanner may split the tablet into to or more. Here body is kept smaller
   * so that rows are written faster, and we can see the expected behaviour.
   *
   * @param dbClient DatabaseClient
   * @param mutationsPerTransaction mutations per transaction.
   * @param numMinutes How long this method should execute in minutes.
   */
  public void performMultiParticipantWrite(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                .to(UUID.randomUUID().toString())
                .set("body")
                .to(String.format("test-body-%s", i))
                .build());
      }
      dbClient.writeWithOptions(
          mutations, Options.tag("app=lab3,env=dev,case=multi-part,action=insert"));
    }
  }

  /** Insert rows to Mailbox and Message tables which are interleaved */
  private void writeMessagesInterleaved(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);

    while (Instant.now().isBefore(doneTime)) {
      for (int mailbox = 0; mailbox < numMailboxes; ++mailbox) {
        List<Mutation> mutations = new ArrayList<>();
        for (int i = 0; i < mutationsPerTransaction; ++i) {
          mutations.add(
              Mutation.newInsertBuilder("Message")
                  .set("sid")
                  .to(mailbox)
                  .set("msg_id")
                  .to(UUID.randomUUID().toString())
                  .set("body")
                  .to(String.format("test-body-%s", i))
                  .set("send_timestamp")
                  .to(Value.COMMIT_TIMESTAMP)
                  .build());
        }
        try {
          dbClient.writeWithOptions(
              mutations, Options.tag("app=lab3,env=dev,case=wr-interleave,action=insert"));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void createDatabase(
      DatabaseAdminClient dbAdminClient, DatabaseId id, List<String> schema) {
    OperationFuture<Database, CreateDatabaseMetadata> op =
        dbAdminClient.createDatabase(id.getInstanceId().getInstance(), id.getDatabase(), schema);
    try {
      // Initiate the request which returns an OperationFuture.
      Database db = op.get();
      System.out.println("Created database [" + db.getId() + "]");
    } catch (ExecutionException e) {
      // If the operation failed during execution, expose the cause.
      throw (SpannerException) e.getCause();
    } catch (InterruptedException e) {
      // Throw when a thread is waiting, sleeping, or otherwise occupied,
      // and the thread is interrupted, either before or during the activity.
      throw SpannerExceptionFactory.propagateInterrupt(e);
    }
  }

  private void executeTasksInParallel(Runnable task, int numProcessors) {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(numProcessors);
    for (int i = 0; i < numProcessors; ++i) {
      final int threadCount = i;
      CompletableFuture<Void> future =
          CompletableFuture.runAsync(task, executorService)
              .exceptionally(
                  ex -> {
                    System.err.println(
                        "Error in Thread ::" + threadCount + " Exception::" + ex.getMessage());
                    return null;
                  });
      futures.add(future);
    }
    CompletableFuture<Void> allOf =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    try {
      allOf.get(); // Wait for all threads to complete
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      executorService.shutdown();
    }
  }

  /**
   * Method to execute the transaction locking or non locking method.
   *
   * @param dbClient DatabaseClient
   * @param isLocking value to decide which method to call. True means locking method will be
   *     called, false means the otherwise.
   */
  private void doWorkSingleTransactionSerial(DatabaseClient dbClient, boolean isLocking) {
    boolean workRemaining = true;
    while (workRemaining) {
      workRemaining =
          isLocking
              ? doWorkSingleTransactionLocking(dbClient)
              : doWorkSingleTransactionNonLocking(dbClient);
    }
  }
  /*
  This method helps reproduce the Lock scenario in Spanner. This method will be executed from multiple thread
  and due to the nature of readWriteTransaction this kind of work can not be expedited because of parallelism.
  It's running the user query  which is using a limit 100 but eventually will lock the whole table due to the full table scan.
  We are also writing the rows value in the table in same transaction so only one thread at a time gets the lock to write
  will succeed.
   */
  private boolean doWorkSingleTransactionLocking(DatabaseClient dbClient) {

    String stmt = "SELECT * FROM WorkList WHERE is_done is false LIMIT 100";
    return Boolean.TRUE.equals(
        dbClient
            // In this logic read and write is happening in same transaction.
            // Setting the transaction tag for main transaction
            // This tag will be added as transaction tag and will be shown in
            // TXN_STATS. tables like TXN_STATS_TOP_MINUTE
            .readWriteTransaction(Options.tag("app=lab3,env=dev,case=locking-method"))
            .run(
                transaction -> {
                  List<String> idList = new ArrayList<>();
                  // Executing the above query to get 100 row where is_done is false, it will result
                  // a full table scan
                  ResultSet resultSet =
                      transaction.executeQuery(
                          Statement.of(stmt),
                          // Setting the transaction for specific action
                          // This tag will be added as request tag and will be shown in
                          // QUERY_STATS. tables like QUERY_STATS_TOP_MINUTE
                          Options.tag("app=lab3,env=dev,case=locking,action=select"));
                  // fetching the id value of each row to save in the list.
                  while (resultSet.next()) {
                    idList.add(resultSet.getString("id"));
                  }
                  resultSet.close();
                  // return false if there are no rows.
                  if (idList.size() == 0) return false;

                  // finding a random number between 1 and the size of list and using that
                  // to get the id at that position from the list. This is just a way to select a
                  // random id out of 100 to process. As it will be executed in multithreading
                  // environment each thread may get different rows as much as possible.
                  // One thread at a time will just update a single row.
                  int randIdx = new Random().nextInt(idList.size());
                  String id = idList.get(randIdx);
                  System.out.printf("Doing work: %s\n", id);
                  // updating the rows is_done value to true in the same transaction.
                  transaction.buffer(
                      Mutation.newUpdateBuilder("WorkList")
                          .set("id")
                          .to(id)
                          .set("is_done")
                          .to(true)
                          .set("timestamp")
                          .to(Value.COMMIT_TIMESTAMP)
                          .build());
                  return true;
                }));
  }

  /*
  This method helps reproduce the non Locking scenario, it's the correct way using read and write in Spanner. This method
  will be executed from multiple thread but here we are executing the limit query in separate read transaction(as it does the full table scan)
  and updating the rows value in separate readWriteTransaction. We are also performing a select to validate the row value but that select is on
  primary key column, so it does not do the full table scan and avoids get table level lock. It's running the user query
  which is using a limit 100 in separate read transaction.
   */
  private boolean doWorkSingleTransactionNonLocking(DatabaseClient dbClient) {

    String stmt = "SELECT * FROM WorkList WHERE is_done is false LIMIT 100";
    List<String> idList = new ArrayList<>();
    // Reading the data in seperate read only transaction.
    try (ReadOnlyTransaction transaction = dbClient.readOnlyTransaction()) {
      ResultSet queryResultSet =
          transaction.executeQuery(
              Statement.of(stmt),
              // Setting the transaction for specific action
              // This tag will be added as request tag and will be shown in
              // QUERY_STATS. tables like QUERY_STATS_TOP_MINUTE
              Options.tag("app=lab3,env=dev,case=non-locking,action=read-select"));
      // fetching the id value of each row to save in the list.
      while (queryResultSet.next()) {
        idList.add(queryResultSet.getString("id"));
      }
      queryResultSet.close();
    }

    // return false if there are no rows.
    if (idList.size() == 0) return false;

    // Finding a random number between 1 and the size of list and using that
    // to get the id at that position from the list. This is just a way to select a
    // random id out of 100 to process. As it will be executed in multithreading
    // environment each thread may get different rows as much as possible.
    int randIdx = new Random().nextInt(idList.size());
    String id = idList.get(randIdx);

    return Boolean.TRUE.equals(
        dbClient
            // starting a read write transaction
            // This tag will be added as transaction tag and will be shown in
            // TXN_STATS. tables like TXN_STATS_TOP_MINUTE
            .readWriteTransaction(Options.tag("app=lab3,env=dev,case=non-locking-method"))
            .run(
                transaction -> {
                  // Reading a specific row based on the primary key column Id from the table, but
                  // this will not lock the table as it's not the full table scan.
                  Struct idRow =
                      transaction.readRow(
                          "WorkList",
                          Key.of(id), // Read single row in a table.
                          Arrays.asList("id", "is_done", "timestamp"));

                  if (idRow == null) {
                    throw new RuntimeException("Table 'Worklist' is empty");
                  }

                  if (idRow.getBoolean("is_done")) return false;
                  // Updating the is_done value to true for the same row.
                  System.out.printf("Doing work: %s\n", id);
                  transaction.buffer(
                      Mutation.newUpdateBuilder("WorkList")
                          .set("id")
                          .to(idRow.getValue("id"))
                          .set("is_done")
                          .to(true)
                          .set("timestamp")
                          .to(Value.COMMIT_TIMESTAMP)
                          .build());
                  return true;
                }));
  }
}
