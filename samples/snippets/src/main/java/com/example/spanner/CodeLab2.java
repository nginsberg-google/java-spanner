package com.example.spanner;

import com.google.cloud.spanner.*;
import com.google.common.collect.ImmutableList;

import java.util.*;

/**
 * This class contains all the method and helper method for performing the steps defined for Spanner
 * Lab2
 */
public class CodeLab2 {

  public void hubbleCreateJoinTables(DatabaseAdminClient dbAdminClient, DatabaseId id) {
    try {
      dbAdminClient
          .updateDatabaseDdl(
              id.getInstanceId().getInstance(),
              id.getDatabase(),
              ImmutableList.of(
                  "CREATE TABLE\n"
                      + "  Person ( person_id STRING(MAX) NOT NULL,\n"
                      + "    name STRING(MAX),\n"
                      + "    age INT64,\n"
                      + "    timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true))\n"
                      + "PRIMARY KEY\n"
                      + "  (person_id)\n",
                  "CREATE TABLE\n"
                      + "  Contact ( contact_id STRING(MAX) NOT NULL,\n"
                      + "    email STRING(MAX),\n"
                      + "    phone STRING(MAX),\n"
                      + "     timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true))\n"
                      + "PRIMARY KEY\n"
                      + "  (contact_id)\n",
                  "CREATE TABLE\n"
                      + "  Address ( address_id STRING(MAX) NOT NULL,\n"
                      + "    person_id STRING(MAX) NOT NULL,\n"
                      + "    address STRING(MAX),\n"
                      + "    city STRING(MAX),\n"
                      + "    state STRING(MAX),\n"
                      + "    zip STRING(MAX),\n"
                      + "     timestamp TIMESTAMP  OPTIONS (allow_commit_timestamp=true))\n"
                      + "    PRIMARY KEY\n"
                      + "  (address_id)"),
              null)
          .get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    System.out.println("Join tables(Person, Contact, Address) successfully got created)");
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
  public void hubbleInsertTestDataToJoinTables(
      DatabaseClient dbClient, int mutationsPerTransaction, int totalRows) {
    System.out.println("Starting execution of hubbleInsertTestDataToJoinTables");
    long startTime = System.currentTimeMillis();

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
      //      System.out.println("started db write");
      // Writing all the rows in different tables.
      dbClient.write(mutationsPerson);
      dbClient.write(mutationsContact);
      dbClient.write(mutationsAddress);
      //      System.out.println("Finshed db write");

    }
    System.out.println(
        "Total time taken in writing "
            + totalRows
            + " rows is "
            + (System.currentTimeMillis() - startTime)
            + " ms");
    System.out.println("Finished execution of hubbleInsertTestDataToJoinTables");
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

    // Generating area code (3 digits)
    int areaCode = random.nextInt(999); // Random area code between 200 and 999

    // Generating exchange code (3 digits)
    int exchangeCode = random.nextInt(999); // Random exchange code between 100 and 999

    // Generating subscriber number (4 digits)
    int subscriberNumber = random.nextInt(9999); // Random subscriber number between 1000 and 9999

    return String.format("(%03d) %03d-%04d", areaCode, exchangeCode, subscriberNumber);
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

    Random random = new Random();
    int randomIndex = random.nextInt(usStates.length);
    return usStates[randomIndex];
  }
}
