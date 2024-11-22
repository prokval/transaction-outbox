package com.gruelbox.transactionoutbox.spring.example;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventuallyConsistentControllerTest extends AbstractSpringBootTest {

  @LocalServerPort
  private int port;

  private URL base;

  @Autowired
  private TestRestTemplate template;

  @Autowired
  private ExternalQueueService externalQueueService;

  @Autowired
  private ExternalQueueServiceResultHandler externalQueueServiceResultHandler;

  @BeforeEach
  void setUp() throws Exception {
    this.base = new URL("http://localhost:" + port + "/");
    externalQueueService.clear();
    externalQueueServiceResultHandler.clear();
  }

  Customer joe = new Customer(1L, "Joe", "Strummer");
  Customer dave = new Customer(2L, "Dave", "Grohl");
  Customer neil = new Customer(3L, "Neil", "Diamond");
  Customer tupac = new Customer(4L, "Tupac", "Shakur");
  Customer jeff = new Customer(5L, "Jeff", "Mills");


  @Test
  void test_01_CheckCustomerCreation() throws Exception {

    var url = base.toString() + "/customer";
    assertTrue(template.postForEntity(url, joe, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, neil, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, jeff, Void.class).getStatusCode().is2xxSuccessful());

    await().atMost(10, SECONDS).pollDelay(1, SECONDS)
        .untilAsserted(() -> assertThat(externalQueueService.getSent()).containsExactlyInAnyOrder(joe, dave, neil, tupac, jeff));

    await().atMost(5, SECONDS).untilAsserted(() -> assertThat(externalQueueServiceResultHandler.getRecords()).containsExactlyInAnyOrder(
            new ExternalQueueServiceResultHandler.ResultRecord(joe, null, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(dave, null, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(neil, null, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(tupac, null, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(jeff, null, null, "Temporary failure, try again"),

            new ExternalQueueServiceResultHandler.ResultRecord(joe, null, joe.getId(), null),
            new ExternalQueueServiceResultHandler.ResultRecord(dave, null, dave.getId(), null),
            new ExternalQueueServiceResultHandler.ResultRecord(neil, null, neil.getId(), null),
            new ExternalQueueServiceResultHandler.ResultRecord(tupac, null, tupac.getId(), null),
            new ExternalQueueServiceResultHandler.ResultRecord(jeff, null, jeff.getId(), null)
    ));
  }

  @Test
  void test_02_CheckAddPayments() {

    var url = base.toString() + "/customer/payment/{value}";
    assertTrue(template.postForEntity(url, joe, Void.class, 30).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class, 11).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, joe, Void.class, 25).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class, 15).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, joe, Void.class, 100).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, neil, Void.class, 10).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, joe, Void.class, 25).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class, 55).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class, 3).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, jeff, Void.class, 1).getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class, 10).getStatusCode().is2xxSuccessful());

    Map<Long, List<Integer>> expected = Map.of(
            joe.getId(), List.of(30, 25, 100, 25),
            dave.getId(), List.of(11, 15, 3),
            neil.getId(), List.of(10),
            tupac.getId(), List.of(55, 10),
            jeff.getId(), List.of(1)
    );
    await().atMost(10, SECONDS).pollDelay(1, SECONDS)
        .untilAsserted(() -> assertThat(externalQueueService.getPayments()).containsAllEntriesOf(expected));

    await().atMost(5, SECONDS).untilAsserted(() -> assertThat(externalQueueServiceResultHandler.getRecords()).containsExactlyInAnyOrder(
            new ExternalQueueServiceResultHandler.ResultRecord(joe, 30, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(dave, 11, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(neil, 10, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(tupac, 55, null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(jeff, 1, null, "Temporary failure, try again"),

            new ExternalQueueServiceResultHandler.ResultRecord(joe, 30, 30, null),
            new ExternalQueueServiceResultHandler.ResultRecord(joe, 25, 55, null),
            new ExternalQueueServiceResultHandler.ResultRecord(joe, 100, 155, null),
            new ExternalQueueServiceResultHandler.ResultRecord(joe, 25, 180, null),

            new ExternalQueueServiceResultHandler.ResultRecord(dave, 11, 11, null),
            new ExternalQueueServiceResultHandler.ResultRecord(dave, 15, 26, null),
            new ExternalQueueServiceResultHandler.ResultRecord(dave, 3, 29, null),

            new ExternalQueueServiceResultHandler.ResultRecord(neil, 10, 10, null),

            new ExternalQueueServiceResultHandler.ResultRecord(tupac, 55, 55, null),
            new ExternalQueueServiceResultHandler.ResultRecord(tupac, 10, 65, null),

            new ExternalQueueServiceResultHandler.ResultRecord(jeff, 1, 1, null)
    ));

  }

  @Test
  void test_03_CheckUpdateStatus() {

    var url = base.toString() + "/customer/status/{status}";
    assertTrue(template.postForEntity(url, joe, Void.class, "ST1").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class, "ST1").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, joe, Void.class, "ST2").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class, "ST2").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, joe, Void.class, "ST3").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, neil, Void.class, "FIN").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, joe, Void.class, "FIN").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class, "ST1").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, dave, Void.class, "FIN").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, jeff, Void.class, "FIN").getStatusCode().is2xxSuccessful());
    assertTrue(template.postForEntity(url, tupac, Void.class, "FIN").getStatusCode().is2xxSuccessful());

    Map<Long, List<String>> expected = Map.of(
            joe.getId(), List.of("FIN"),
            dave.getId(), List.of("FIN"),
            neil.getId(), List.of("FIN"),
            tupac.getId(), List.of("FIN"),
            jeff.getId(), List.of("FIN")
    );
    await().atMost(10, SECONDS).pollDelay(1, SECONDS)
        .untilAsserted(() -> assertThat(externalQueueService.getStatuses()).containsAllEntriesOf(expected));

    await().atMost(5, SECONDS).untilAsserted(() -> assertThat(externalQueueServiceResultHandler.getRecords()).containsExactlyInAnyOrder(
            new ExternalQueueServiceResultHandler.ResultRecord(joe, "FIN", null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(dave, "FIN", null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(neil, "FIN", null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(tupac, "FIN", null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(jeff, "FIN", null, "Temporary failure, try again"),
            new ExternalQueueServiceResultHandler.ResultRecord(joe, "FIN", null, null),
            new ExternalQueueServiceResultHandler.ResultRecord(dave, "FIN", null, null),
            new ExternalQueueServiceResultHandler.ResultRecord(neil, "FIN", null, null),
            new ExternalQueueServiceResultHandler.ResultRecord(tupac, "FIN", null, null),
            new ExternalQueueServiceResultHandler.ResultRecord(jeff, "FIN", null, null)
    ));
  }
}
