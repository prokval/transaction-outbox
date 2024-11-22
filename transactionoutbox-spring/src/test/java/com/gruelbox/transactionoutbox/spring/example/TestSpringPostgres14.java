package com.gruelbox.transactionoutbox.spring.example;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

@Testcontainers
public class TestSpringPostgres14 extends AbstractSpringTest {

    @Container
    @SuppressWarnings({"rawtypes", "resource"})
    static final PostgreSQLContainer postgres =
            (PostgreSQLContainer)
                    new PostgreSQLContainer("postgres:14")
                            .withStartupTimeout(Duration.ofHours(1))
                            .withReuse(true);


    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("outbox.sqlDialect", () -> "POSTGRESQL_SEQ");
    }


}
