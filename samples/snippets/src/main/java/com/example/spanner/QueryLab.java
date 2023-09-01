package com.example.spanner;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.common.collect.ImmutableList;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class contains all the method and helper method for performing the steps defined for Spanner
 * Query Lab
 */
public class QueryLab {

  private static final String LOREM_IPSUM =
      "Aenean sit amet lectus est. Nullam ornare ligula luctus auctor placerat. Morbi fermentum volutpat massa, sit amet consectetur metus vehicula sed. Sed auctor scelerisque tempus. Morbi hendrerit tortor et felis scelerisque, at fermentum risus posuere. Suspendisse finibus, tellus eu ullamcorper posuere, odio ex pulvinar est, a malesuada libero turpis quis massa. Quisque placerat lacus orci. Proin ac viverra metus. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus cursus rhoncus iaculis. Pellentesque nisl tellus, venenatis nec erat sit amet, eleifend porttitor nisl. Maecenas fringilla ex id mauris facilisis, sed luctus dui elementum. Suspendisse ac orci lectus. Suspendisse vitae sapien convallis, commodo leo ut, ultricies arcu. Fusce pellentesque sem vestibulum, sodales purus eget, auctor odio. Ut et nunc metus. Aenean ac ex faucibus, tristique nibh ut, euismod lorem. Fusce a ex ut nibh consectetur mollis. Aenean suscipit elit dui, faucibus vestibulum leo commodo a. Nulla ultricies vitae velit cursus commodo. Morbi eu sapien in magna condimentum porta quis eget sem. Etiam tempor auctor diam, quis mollis odio scelerisque et. Fusce tempus mauris mi, et varius enim condimentum in. Aliquam nisi lorem, pulvinar id ullamcorper vitae, fringilla vel leo. Fusce vehicula tincidunt vulputate. Vivamus efficitur nunc quis lorem porttitor elementum. Donec ex neque, vestibulum nec mollis quis, lacinia quis dui. Nullam rhoncus quis lacus nec euismod. Vivamus porttitor sem nec nisl auctor ultrices. Vivamus non laoreet lectus. Aliquam condimentum semper libero eu elementum. Nullam lobortis ultricies gravida. Integer in lacinia lacus, ac consequat magna. Suspendisse et risus vel diam facilisis ornare a in arcu. Nulla nec nunc sem. Cras aliquam nulla sem, luctus maximus est gravida ut. Ut pellentesque pharetra convallis. Quisque molestie, ipsum sit amet scelerisque convallis, magna ante fringilla massa, in blandit turpis nibh ornare magna. Curabitur mi tortor, feugiat id sem ac, scelerisque congue tortor. Aenean non viverra risus. Praesent vel enim quis dolor auctor aliquet. Maecenas faucibus mi at venenatis suscipit. Integer interdum magna vitae mauris interdum, laoreet ullamcorper erat tincidunt. Morbi vel ipsum convallis, semper quam vitae, sagittis tellus. In facilisis eu lorem imperdiet laoreet. Suspendisse gravida a magna et condimentum. Suspendisse vitae risus vitae est pulvinar convallis at sit amet sem. Morbi vel imperdiet leo, sit amet cursus urna. Pellentesque suscipit ut neque non laoreet. Duis non ipsum ipsum. Quisque ut porttitor dui. Duis nulla augue, varius quis tellus sed, efficitur bibendum justo. Cras vestibulum congue ante in gravida. Quisque tincidunt nisi nisl, sed vulputate leo luctus ac. Pellentesque quis tempor leo, sed faucibus ipsum. Donec rutrum turpis nec auctor lobortis. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Sed lobortis sodales risus vitae eleifend. Nullam pharetra dapibus lobortis. Vestibulum nisl diam, dignissim sit amet lacus sit amet, venenatis auctor nisi. Phasellus sollicitudin tortor a mi fringilla, imperdiet tristique ex feugiat. Nulla ut vehicula metus, nec tempus odio. Phasellus pellentesque lacus lacus, fringilla egestas felis hendrerit ac. Nam suscipit orci eros, at molestie nunc tincidunt id. Aliquam maximus finibus leo. Vestibulum orci turpis, fringilla eget lorem id, cursus consequat tortor. Suspendisse efficitur purus lectus, ac ornare sem lobortis a. Interdum et malesuada fames ac ante ipsum primis in faucibus. Sed sodales id mi eu aliquam. Curabitur enim libero, tempor vel vestibulum id, eleifend at lacus. Maecenas ut feugiat justo, nec dictum justo. Nulla porttitor accumsan rhoncus. Cras ligula velit, molestie sit amet molestie sed, commodo et libero. Etiam eu condimentum arcu. Aliquam erat volutpat. Nullam sit amet urna ipsum. Vestibulum at velit id velit tincidunt ultricies a ut metus. In vitae mollis diam. Proin lacinia fringilla purus vitae aliquet. Vestibulum a tincidunt eros. Aenean imperdiet aliquet arcu, vitae euismod turpis fermentum quis. Orci varius natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Suspendisse potenti. Pellentesque quis efficitur mauris, pulvinar sagittis ante. Cras efficitur porta convallis. Nullam eleifend congue finibus. Nam sodales tincidunt odio ac elementum. Sed sem risus, imperdiet quis suscipit eu, imperdiet vitae nibh. Morbi gravida neque ac sodales varius. In convallis massa vel lectus fermentum ultricies ac vitae eros. Sed justo sem, dignissim sit amet tempus ultrices, pellentesque at libero. Quisque a quam volutpat, tristique erat a, auctor enim. Suspendisse finibus arcu erat, nec mattis tortor facilisis in. Donec ornare mattis lorem, in interdum turpis venenatis vel. Mauris tempus porttitor libero. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vivamus eget tempor dui. Nunc et elit eu eros tincidunt sapien.";

  private static final String SUBJECT = "Random subject for the email for testing purpose %s";

  private static final int NUM_PROCESSORS = Runtime.getRuntime().availableProcessors();

  public void createInterleaved(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    createDatabase(
        dbAdminClient,
        id,
        Arrays.asList(
            "CREATE TABLE Mailbox("
                + "sid INT64 NOT NULL,"
                + "guid STRING(MAX),"
                + "state STRING(MAX),"
                + ") PRIMARY KEY (sid)",
            "CREATE TABLE Message("
                + "sid INT64 NOT NULL,"
                + "msg_id STRING(MAX) NOT NULL,"
                + "subject STRING(MAX),"
                + "body STRING(MAX),"
                + "send_timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true),"
                + ") PRIMARY KEY (sid, msg_id),"
                + "INTERLEAVE IN PARENT Mailbox ON DELETE CASCADE"));
  }

  /** Writing the value in the Mailbox table. */
  public void writeMailboxes(DatabaseClient dbClient, int numMailboxes) {
    List<Mutation> mutations = new ArrayList<>();
    for (int sid = 0; sid < numMailboxes; ++sid) {
      mutations.add(
          Mutation.newInsertBuilder("Mailbox")
              .set("sid")
              .to(sid)
              .set("guid")
              .to(UUID.randomUUID().toString())
              .build());
    }
    try {
      dbClient.write(mutations);
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
   * This method will create all the table mentioned given below, in the existing database. It will
   * not create the database.
   */
  public void createJoinTables(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    try {
      dbAdminClient
          .updateDatabaseDdl(
              id.getInstanceId().getInstance(),
              id.getDatabase(),
              ImmutableList.of(
                  "CREATE TABLE "
                      + "  Person ( person_id STRING(MAX) NOT NULL,"
                      + "    name STRING(MAX),"
                      + "    age INT64,"
                      + "    timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true))"
                      + "PRIMARY KEY"
                      + "  (person_id)",
                  "CREATE TABLE"
                      + "  Contact ( contact_id STRING(MAX) NOT NULL,"
                      + "    email STRING(MAX),"
                      + "    phone STRING(MAX),"
                      + "     timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true))"
                      + "PRIMARY KEY"
                      + "  (contact_id)",
                  "CREATE TABLE\n"
                      + "  Address ( address_id STRING(MAX) NOT NULL,"
                      + "    person_id STRING(MAX) NOT NULL,"
                      + "    address STRING(MAX),"
                      + "    city STRING(MAX),"
                      + "    state STRING(MAX),"
                      + "    zip STRING(MAX),"
                      + "     timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true))"
                      + "    PRIMARY KEY"
                      + "  (address_id)"),
              null)
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * This method will insert data in the three non adjacent table tables which will be used to
   * simulate correct join scenario. Its inserting rows in Person, Contact and Address table.
   * Contact will get 70% common row with Person and Address will get 20% common rows with Contact.
   * All table will get the same row count rest all the rows will be non-unique.It takes approx 13
   * min to write 100k rows.
   *
   * @param dbClient {@link DatabaseClient} to perform spanner operation.
   * @param mutationsPerTransaction Number of mutations for one transaction.
   * @param totalRows Total number of rows to be inserted.
   */
  public void insertTestDataToJoinTables(
      DatabaseClient dbClient, int mutationsPerTransaction, int totalRows) {

    int rowCount = 0;
    while (rowCount < totalRows) {
      List<Mutation> mutationsPerson = new ArrayList<>();
      List<Mutation> mutationsContact = new ArrayList<>();
      List<Mutation> mutationsAddress = new ArrayList<>();
      for (int i = 0; i < mutationsPerTransaction; ++i) {
        int seventyPercentRows = (int) (mutationsPerTransaction * 0.7);
        int twentyPercentRows = (int) (mutationsPerTransaction * 0.2);
        String personId = UUID.randomUUID().toString();
        String contactId = UUID.randomUUID().toString();
        String addressId = UUID.randomUUID().toString();
        String tempPersonId = UUID.randomUUID().toString();
        // Making sure that 70% of person and contact rows are common in terms of Id which will be
        // used for join.
        if (i < seventyPercentRows) {
          contactId = personId;
        }
        // Making sure that 20% of address and contact, address and person rows are common in terms
        // of id which will be used for join.
        if (i < twentyPercentRows) {
          addressId = contactId;
          tempPersonId = personId;
        }

        mutationsPerson.add(getPersonTableDummyValue(personId));
        mutationsContact.add(getContactTableDummyValue(contactId));
        mutationsAddress.add(getAddressTableDummyValue(addressId, tempPersonId));
        rowCount++;
      }
      // Writing all the rows in different tables.
      dbClient.write(mutationsPerson);
      dbClient.write(mutationsContact);
      dbClient.write(mutationsAddress);
    }
  }

  /**
   * Helper method to generate the test data for Person table.
   *
   * @param personId unique Id value.
   * @return @{@link Mutation} object
   */
  private Mutation getPersonTableDummyValue(String personId) {
    return Mutation.newInsertBuilder("Person")
        .set("person_id")
        .to(personId)
        .set("name")
        .to(UUID.randomUUID().toString())
        .set("age")
        .to(new Random().nextInt(100 - 10 + 1) + 10)
        .set("timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  /**
   * Helper method to generate the test data for Contact table.
   *
   * @param contactId unique Id value for contactId.
   * @return @{@link Mutation} object
   */
  private Mutation getContactTableDummyValue(String contactId) {
    return Mutation.newInsertBuilder("Contact")
        .set("contact_id")
        .to(contactId)
        .set("email")
        .to(
            String.format(
                "test%s@%stest.com", new Random().nextInt(100), new Random().nextInt(1000)))
        .set("phone")
        .to(generateRandomPhoneNumber())
        .set("timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  /**
   * Helper method to generate the test data for Address table.
   *
   * @param addressId unique Id value for address table.
   * @param personId unique id value of the person table which is also save in person table.
   * @return @{@link Mutation} object
   */
  private Mutation getAddressTableDummyValue(String addressId, String personId) {
    return Mutation.newInsertBuilder("Address")
        .set("address_id")
        .to(addressId)
        .set("person_id")
        .to(personId)
        .set("address")
        .to(UUID.fromString("123e4567-e89b-12d3-a456-426655440000").toString())
        .set("zip")
        .to(new Random().nextInt(99999) + 10000)
        .set("city")
        .to("test" + generateRandomState())
        .set("state")
        .to(generateRandomState())
        .set("timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  /** Helper method to generate the random phone number. */
  private String generateRandomPhoneNumber() {
    Random random = new Random();
    return String.format(
        "(%03d) %03d-%04d", random.nextInt(999), random.nextInt(999), random.nextInt(9999));
  }

  /** Helper method to generate the random us states. */
  private String generateRandomState() {
    String[] usStates = {
      "Alabama",
      "Alaska",
      "Arizona",
      "Arkansas",
      "California",
      "Colorado",
      "Connecticut",
      "Delaware",
      "Florida",
      "Georgia",
      "Hawaii",
      "Idaho",
      "Illinois",
      "Indiana",
      "Iowa",
      "Kansas",
      "Kentucky",
      "Louisiana",
      "Maine",
      "Maryland",
      "Massachusetts",
      "Michigan",
      "Minnesota",
      "Mississippi",
      "Missouri",
      "Montana",
      "Nebraska",
      "Nevada",
      "New Hampshire",
      "New Jersey",
      "New Mexico",
      "New York",
      "North Carolina",
      "North Dakota",
      "Ohio",
      "Oklahoma",
      "Oregon",
      "Pennsylvania",
      "Rhode Island",
      "South Carolina",
      "South Dakota",
      "Tennessee",
      "Texas",
      "Utah",
      "Vermont",
      "Virginia",
      "Washington",
      "West Virginia",
      "Wisconsin",
      "Wyoming"
    };
    return usStates[new Random().nextInt(usStates.length)];
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
      allOf.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    } finally {
      executorService.shutdown();
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
}
