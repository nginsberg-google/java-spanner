package com.example.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

/**
 * This class contains all the method and helper method for performing the steps defined for Spanner
 * Lab1
 */
public class HubbleTransactionsCodeLab {

  private static final String LOREM_IPSUM =
      "Aenean sit amet lectus est. Nullam ornare ligula luctus auctor placerat. Morbi fermentum volutpat massa, sit amet consectetur metus vehicula sed. Sed auctor scelerisque tempus. Morbi hendrerit tortor et felis scelerisque, at fermentum risus posuere. Suspendisse finibus, tellus eu ullamcorper posuere, odio ex pulvinar est, a malesuada libero turpis quis massa. Quisque placerat lacus orci. Proin ac viverra metus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus cursus rhoncus iaculis. Pellentesque nisl tellus, venenatis nec erat sit amet, eleifend porttitor nisl. Maecenas fringilla ex id mauris facilisis, sed luctus dui elementum. Suspendisse ac orci lectus. Suspendisse vitae sapien convallis, commodo leo ut, ultricies arcu. Fusce pellentesque sem vestibulum, sodales purus eget, auctor odio. Ut et nunc metus. Aenean ac ex faucibus, tristique nibh ut, euismod lorem. Fusce a ex ut nibh consectetur mollis. Aenean suscipit elit dui, faucibus vestibulum leo commodo a. Nulla ultricies vitae velit cursus commodo. Morbi eu sapien in magna condimentum porta quis eget sem. Etiam tempor auctor diam, quis mollis odio scelerisque et. Fusce tempus mauris mi, et varius enim condimentum in. Aliquam nisi lorem, pulvinar id ullamcorper vitae, fringilla vel leo. Fusce vehicula tincidunt vulputate. Vivamus efficitur nunc quis lorem porttitor elementum. Donec ex neque, vestibulum nec mollis quis, lacinia quis dui. Nullam rhoncus quis lacus nec euismod. Vivamus porttitor sem nec nisl auctor ultrices. Vivamus non laoreet lectus. Aliquam condimentum semper libero eu elementum. Nullam lobortis ultricies gravida. Integer in lacinia lacus, ac consequat magna. Suspendisse et risus vel diam facilisis ornare a in arcu. Nulla nec nunc sem. Cras aliquam nulla sem, luctus maximus est gravida ut. Ut pellentesque pharetra convallis. Quisque molestie, ipsum sit amet scelerisque convallis, magna ante fringilla massa, in blandit turpis nibh ornare magna. Curabitur mi tortor, feugiat id sem ac, scelerisque congue tortor. Aenean non viverra risus. Praesent vel enim quis dolor auctor aliquet. Maecenas faucibus mi at venenatis suscipit. Integer interdum magna vitae mauris interdum, laoreet ullamcorper erat tincidunt. Morbi vel ipsum convallis, semper quam vitae, sagittis tellus. In facilisis eu lorem imperdiet laoreet. Suspendisse gravida a magna et condimentum. Suspendisse vitae risus vitae est pulvinar convallis at sit amet sem. Morbi vel imperdiet leo, sit amet cursus urna. Pellentesque suscipit ut neque non laoreet. Duis non ipsum ipsum. Quisque ut porttitor dui. Duis nulla augue, varius quis tellus sed, efficitur bibendum justo. Cras vestibulum congue ante in gravida. Quisque tincidunt nisi nisl, sed vulputate leo luctus ac. Pellentesque quis tempor leo, sed faucibus ipsum. Donec rutrum turpis nec auctor lobortis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Sed lobortis sodales risus vitae eleifend. Nullam pharetra dapibus lobortis. Vestibulum nisl diam, dignissim sit amet lacus sit amet, venenatis auctor nisi. Phasellus sollicitudin tortor a mi fringilla, imperdiet tristique ex feugiat. Nulla ut vehicula metus, nec tempus odio. Phasellus pellentesque lacus lacus, fringilla egestas felis hendrerit ac. Nam suscipit orci eros, at molestie nunc tincidunt id. Aliquam maximus finibus leo. Vestibulum orci turpis, fringilla eget lorem id, cursus consequat tortor. Suspendisse efficitur purus lectus, ac ornare sem lobortis a. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed sodales id mi eu aliquam. Curabitur enim libero, tempor vel vestibulum id, eleifend at lacus. Maecenas ut feugiat justo, nec dictum justo. Nulla porttitor accumsan rhoncus. Cras ligula velit, molestie sit amet molestie sed, commodo et libero. Etiam eu condimentum arcu. Aliquam erat volutpat. Nullam sit amet urna ipsum. Vestibulum at velit id velit tincidunt ultricies a ut metus. In vitae mollis diam. Proin lacinia fringilla purus vitae aliquet. Vestibulum a tincidunt eros. Aenean imperdiet aliquet arcu, vitae euismod turpis fermentum quis. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Suspendisse potenti. Pellentesque quis efficitur mauris, pulvinar sagittis ante. Cras efficitur porta convallis. Nullam eleifend congue finibus. Nam sodales tincidunt odio ac elementum. Sed sem risus, imperdiet quis suscipit eu, imperdiet vitae nibh. Morbi gravida neque ac sodales varius. In convallis massa vel lectus fermentum ultricies ac vitae eros. Sed justo sem, dignissim sit amet tempus ultrices, pellentesque at libero. Quisque a quam volutpat, tristique erat a, auctor enim. Suspendisse finibus arcu erat, nec mattis tortor facilisis in. Donec ornare mattis lorem, in interdum turpis venenatis vel. Mauris tempus porttitor libero. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vivamus eget tempor dui. Nunc et elit eu eros tincidunt sapien.";

  private static final String SUBJECT = "Random subject for the email for testing purpose %s";

  private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();

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
                + " sid INT64 NOT NULL,"
                + " msg_id STRING(MAX) NOT NULL,"
                + " subject STRING(MAX),"
                + " body STRING(MAX),"
                + " send_timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true),"
                + ") PRIMARY KEY (sid, msg_id),"
                + "INTERLEAVE IN PARENT Mailbox ON DELETE CASCADE"));
  }

  public void createWorkItems(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE WorkList("
                + " id STRING(MAX) NOT NULL,"
                + " is_done BOOL,"
                + " generated_value STRING(MAX) NOT NULL,"
                + " timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true), "
                + ") PRIMARY KEY (id)"));
  }

  public void writeWorkItems(
      DatabaseClient dbClient, int numWorkItems, int mutationsPerTransaction) {
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
                .set("generated_value")
                .to(UUID.randomUUID().toString())
                .set("timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void writeMailboxes(DatabaseClient dbClient, int numMailboxes) {
    List<Mutation> mutations = new ArrayList<>();
    for (int sid = 0; sid < numMailboxes; ++sid) {
      mutations.add(Mutation.newInsertBuilder("Mailbox").set("sid").to(sid).build());
    }
    try {
      dbClient.write(mutations);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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
                  .set("subject")
                  .to(String.format(SUBJECT, i))
                  .set("body")
                  .to(LOREM_IPSUM)
                  .set("send_timestamp")
                  .to(Value.COMMIT_TIMESTAMP)
                  .build());
        }
        try {
          dbClient.write(mutations);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  public void writeMessagesInterleavedParallel(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> writeMessagesInterleaved(dbClient, numMailboxes, mutationsPerTransaction, numMinutes),
        NUM_PROCESSORS);
  }

  public void writeMessages(DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                .to(Instant.now().toString() + i)
                .set("SUBJECT")
                .to(String.format(SUBJECT, i))
                .set("body")
                .to(LOREM_IPSUM)
                .set("send_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void writeMessagesUUID(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("msg_id")
                .to(Instant.now().toString() + i)
                .set("SUBJECT")
                .to(String.format(SUBJECT, i))
                .set("body")
                .to(LOREM_IPSUM)
                .set("send_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void writeMessagesParallel(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> writeMessages(dbClient, mutationsPerTransaction, numMinutes), NUM_PROCESSORS);
  }

  public void writeMessagesParallelUUID(
      DatabaseClient dbClient, int mutationsPerTransaction, int numMinutes) {
    executeTasksInParallel(
        () -> writeMessagesUUID(dbClient, mutationsPerTransaction, numMinutes), NUM_PROCESSORS);
  }

  public void updatesAndReads(
      DatabaseClient dbClient,
      int numMailboxes,
      int mutationsPerTransaction,
      int numRows,
      int numMinutes,
      boolean isStrong) {
    List<List<String>> keys = new ArrayList<>(numMailboxes);
    for (int i = 0; i < numMailboxes; ++i) {
      keys.add(new ArrayList<>(numRows / numMailboxes));
    }

    for (int sid = 0; sid < numMailboxes; ++sid) {
      for (int i = 0; i < numRows / numMailboxes / mutationsPerTransaction; ++i) {
        List<Mutation> mutations = new ArrayList<>();
        for (int j = 0; j < mutationsPerTransaction; ++j) {
          String uuid = UUID.randomUUID().toString();
          mutations.add(
              Mutation.newInsertBuilder("Message")
                  .set("sid")
                  .to(sid)
                  .set("msg_id")
                  .to(uuid)
                  .set("body")
                  .to(LOREM_IPSUM)
                  .build());
          keys.get(sid).add(uuid);
        }
        dbClient.write(mutations);
      }
    }

    System.out.println("Beginning workload.");
    executeTasksInParallel(
        () -> updates(dbClient, keys, mutationsPerTransaction, numMinutes), NUM_PROCESSORS / 2);
    executeTasksInParallel(
        () -> reads(dbClient, keys, mutationsPerTransaction, numMinutes, isStrong),
        NUM_PROCESSORS / 2);
  }

  public void doWorkSingleTransactionParallelLocking(DatabaseClient dbClient) {
    // getting the available processor
    ExecutorService executorService = Executors.newFixedThreadPool(NUM_PROCESSORS);

    List<Future<Integer>> futures = new ArrayList<>();
    // List to store the count of total number of workItem(row) which has not yet completed means
    // is_done is false.
    List<String> workItemCount = new ArrayList<>();
    // Below logic is reading the generated_value for the rows which are having is_done=false. We
    // have added generated_value column value in incremented order starting from 1 in the table.
    // This will help us to assign executing the random workItem(rows) work without overlapping. As
    // each thread will pick up different generated_value to query.
    try (ResultSet resultSet =
        dbClient
            .singleUse() // Execute a single read or query against Cloud Spanner.
            // Below query can be parameterized to but kept here for simplicity
            .executeQuery(
                Statement.of("SELECT generated_value FROM WorkList WHERE is_done is false"))) {
      while (resultSet.next()) {
        workItemCount.add(resultSet.getString(0));
      }
    }

    if (workItemCount.size() == 0) {
      System.out.println(
          "There is no workItem left to process, please update the is_done value to false, or add more work item in the table");
      return;
    }

    // Assigning each thread the amount of work equally
    for (int i = 0; i < NUM_PROCESSORS; ++i) {
      // Each thread get the index from this loop, total workItem to be processed and number of
      // thread (numProcessors) available for processing.
      // E.g In case of 10 threads and 30 workItem the assignment for this method would be like

      // Thread 1: ThreadIndex(i) 0, workitem 30
      // Thread 2: ThreadIndex(i) 1  workitem 30
      // Thread 3: ThreadIndex(i) 2  workitem 30
      // ...
      // Thread 10: ThreadIndex(i) 9  workitem 30
      // These values will be determined by received by each thread(an object of WorkItemProcessor)
      // while executing based on the input values passed here.
      String lockQueryInTransaction =
          "SELECT * FROM WorkList WHERE is_done is false and generated_value= '%s';";
      Callable<Integer> callable =
          new WorkItemProcessor<>(
              workItemCount, i, NUM_PROCESSORS, dbClient, lockQueryInTransaction);
      // Submitting each thread
      Future<Integer> future = executorService.submit(callable);
      futures.add(future);
    }
    int totalProcessed = 0;
    // Calling the get on the future to wait for thread to finish and get the result.
    try {
      for (Future<Integer> future : futures) {

        totalProcessed += future.get();
      }

      executorService.shutdown();
      // Waiting for all thread to finish
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void doWorkSingleTransactionParallelNonLocking(DatabaseClient dbClient) {
    // getting the available processor
    ExecutorService executorService = Executors.newFixedThreadPool(NUM_PROCESSORS);

    List<Future<Integer>> futures = new ArrayList<>();
    // This is the store the count of total number of workItem(row) which has not yet completed
    // means  is_done is false.
    List<String> workItemCount = new ArrayList<>();
    // Below logic is reading the id(primary key) for the rows which are having is_done=false. This
    // will help us to read the different ids by different thread randomly.
    try (ResultSet resultSet =
        dbClient
            .singleUse() // Execute a single read or query against Cloud Spanner.
            .executeQuery(Statement.of("SELECT id FROM WorkList WHERE is_done is false;"))) {
      while (resultSet.next()) {
        workItemCount.add(resultSet.getString(0));
      }
    }

    if (workItemCount.size() == 0) {
      System.out.println(
          "There is no workItem left to process, please update the is_done value to false, or add more work item in the table");
      return;
    }

    // Assigning each thread the amount of work equally
    for (int i = 0; i < NUM_PROCESSORS; ++i) {
      // Each thread get the index from this loop, total workItem to be processed and number of
      // thread (numProcessors) available for processing.
      // E.g In case of 10 threads and 30 workItem the assignment for this method would be like

      // Thread 1: ThreadIndex(i) 0, workitem 30
      // Thread 2: ThreadIndex(i) 1  workitem 30
      // Thread 3: ThreadIndex(i) 2  workitem 30
      // ...
      // Thread 10: ThreadIndex(i) 9  workitem 30
      // These values will be determined by received by each thread(an object of WorkItemProcessor)
      // while executing based on the input values passed here.
      String nonLockQueryInTransaction =
          "SELECT * FROM WorkList WHERE is_done is false and id= '%s';";
      Callable<Integer> callable =
          new WorkItemProcessor<>(
              workItemCount, i, NUM_PROCESSORS, dbClient, nonLockQueryInTransaction);
      // Submitting each thread
      Future<Integer> future = executorService.submit(callable);
      futures.add(future);
    }
    int totalProcessed = 0;
    // Calling the get on the future to wait for thread to finish and get the result.
    try {
      for (Future<Integer> future : futures) {

        totalProcessed += future.get();
      }
      executorService.shutdown();
      executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeMessagesInterleavedParallel100k(
      DatabaseClient dbClient,
      int numMailboxes,
      int numberOfMessages) {

    final int mutationsPerTransactionTemp = numberOfMessages / numMailboxes / NUM_PROCESSORS;

    executeTasksInParallel(
        () ->
            hubbleWriteMessagesInterleaved100K(dbClient, numMailboxes, mutationsPerTransactionTemp),
        NUM_PROCESSORS);
  }

  public void hubbleWriteMessagesInterleaved100K(
      DatabaseClient dbClient, int numMailboxes, int mutationsPerTransaction) {
    for (int mailbox = 0; mailbox < numMailboxes; ++mailbox) {

      List<Mutation> mutations = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        mutations.add(
            Mutation.newInsertBuilder("Message")
                .set("sid")
                .to(mailbox)
                .set("msg_id")
                .to(UUID.randomUUID().toString())
                .set("subject")
                .to(String.format(SUBJECT, i))
                .set("body")
                .to(LOREM_IPSUM)
                .set("send_timestamp")
                .to(Value.COMMIT_TIMESTAMP)
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
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

  private void updates(
      DatabaseClient dbClient,
      List<List<String>> keys,
      int mutationsPerTransaction,
      int numMinutes) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    Random random = new Random();
    while (Instant.now().isBefore(doneTime)) {
      List<Mutation> mutations = new ArrayList<>();
      int sid = random.nextInt(keys.size());
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        String uuid = keys.get(sid).get(random.nextInt(keys.get(sid).size()));
        mutations.add(
            Mutation.newUpdateBuilder("Message")
                .set("sid")
                .to(sid)
                .set("msg_id")
                .to(uuid)
                .set("body")
                .to("updated!")
                .build());
      }
      try {
        dbClient.write(mutations);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void reads(
      DatabaseClient dbClient,
      List<List<String>> keys,
      int mutationsPerTransaction,
      int numMinutes,
      boolean isStrong) {
    Instant doneTime = Instant.now().plus(numMinutes, ChronoUnit.MINUTES);
    Random random = new Random();
    while (Instant.now().isBefore(doneTime)) {
      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      int sid = random.nextInt(keys.size());
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        String uuid = keys.get(sid).get(random.nextInt(keys.get(sid).size()));
        keySetBuilder.addKey(Key.of(sid, uuid));
      }
      if (isStrong) {
        ResultSet throwaway =
            dbClient.singleUse().read("Message", keySetBuilder.build(), Arrays.asList("body"));
        while (throwaway.next()) {}
      } else {
        ResultSet throwaway =
            dbClient
                .singleUse(TimestampBound.ofMaxStaleness(15, TimeUnit.SECONDS))
                .read("Message", keySetBuilder.build(), Arrays.asList("body"));
        while (throwaway.next()) {}
      }
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
   * Represents a task to process workItem(rows) in parallel, distributing work among threads based
   * on a specific value.
   */
  static class WorkItemProcessor<T> implements Callable<Integer> {
    private final List<T> totalWorkItem;
    private final int threadIndex;
    private final int numThreads;
    private final DatabaseClient dbClient;
    private final String query;

    /**
     * Constructs a WorkItemProcessor with the given parameters.
     *
     * @param totalWorkItem The list of workitems(rows) to be processed.
     * @param numThreads The total number of threads.
     * @param threadIndex The index of the current thread.
     * @param dbClient Spanner DatabaseClient.
     */
    public WorkItemProcessor(
        List<T> totalWorkItem,
        int threadIndex,
        int numThreads,
        DatabaseClient dbClient,
        String query) {
      this.totalWorkItem = totalWorkItem;
      this.threadIndex = threadIndex;
      this.numThreads = numThreads;
      this.dbClient = dbClient;
      this.query = query;
    }

    /**
     * Runs the thread, processing rows based on distribution values.
     *
     * <p>The method iterates through the rows, distributing work among threads based on their
     * indices. Each thread processes rows starting from its {@code threadIndex} and then
     * incrementing by {@code numThreads} for subsequent rows, ensuring even distribution of work
     * among threads. If the number of rows is not a multiple of the number of threads, some threads
     * may process fewer rows.
     */
    public Integer call() {
      int count = 0;
      // Each thread will loop here to process as many rows as possible with in the given limit.
      // E.g Thread 1 will have row=1(startrow/threadIndex), then 11,21,31,41(using logic row +=
      // numThreads)...
      // Thread 2 will have row=2(startrow/threadIndex), then 22,32,42,52(using logic row +=
      // numThreads)...
      // Due to this reason while creating the thread object all these values are passed and
      // startRow value is set.
      for (int i = threadIndex; i <= totalWorkItem.size(); i += numThreads) {

        if (i < totalWorkItem.size()) {
          T row = totalWorkItem.get(i);
          processWorkItem(row, dbClient);
        }
        count++;
      }
      return count;
    }

    /*
        Method to process the workItem using the readWriteTransaction(). In case of a locking method, you may observe
        extended processing time for the line "Doing work Id: ..", involving the same 'generated_value' and 'id'. This behavior
        stems from issues like lock contention and other locking-related problems in Spanner. As a result,
        Spanner can't complete thread execution within a single execution cycle (which is the expected behavior for this test).

        Despite using a specific value for row processing, the 'column' isn't indexed. Consequently, a
        full table scan occurs. This is exacerbated by the read/write transaction nature, which prevents
        the realization of multithreading benefits.

        When this method is invoked by another non-locking method that uses the 'id' (primary key) column
        to fetch data, this issue won't be encountered.
    */
    private void processWorkItem(T row, DatabaseClient dbClient) {
      // Select the specific row from the table. generated_value is passing in the query which will
      // fetch a specific row from the table.
      // This way distribution of random processing is easy and deterministic.
      String stmt = String.format(query, row);
      boolean didWork =
          // Starting a readwrite transaction.
          Boolean.TRUE.equals(
              dbClient
                  .readWriteTransaction()
                  .run(
                      transaction -> {
                        ResultSet resultSet = transaction.executeQuery(Statement.of(stmt));
                        while (resultSet.next()) {
                          String id = resultSet.getString("id");
                          String generatedValue = resultSet.getString("generated_value");
                          System.out.printf(
                              "Doing work Id: %s, Generated Value: %s\n", id, generatedValue);
                          transaction.buffer(
                              Mutation.newUpdateBuilder("WorkList")
                                  .set("id")
                                  .to(id)
                                  .set("is_done")
                                  .to(true)
                                  .set("timestamp")
                                  .to(Value.COMMIT_TIMESTAMP)
                                  .build());
                        }
                        return true;
                      }));
//      System.out.println("WorkItem is completed ::" + didWork + ",processing row:" + row);
    }
  }
}
