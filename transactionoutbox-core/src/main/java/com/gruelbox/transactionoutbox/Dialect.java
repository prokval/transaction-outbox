package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

/** The SQL dialects supported by {@link DefaultPersistor}. */
public interface Dialect {
  /**
   * @return Format string for the SQL required to delete expired retained records.
   */
  String getDeleteExpired();

  String getSelectBatch();

  String getLock();

  String getCheckSql();

  String getFetchNextInAllTopics();

  String getFetchCurrentVersion();

  String getFetchNextSequence();

  String booleanValue(boolean criteriaValue);

  void createVersionTableIfNotExists(Connection connection) throws SQLException;

  Stream<Migration> getMigrations();

  Dialect MY_SQL_5 = DefaultDialect.builder("MY_SQL_5").build();
  Dialect MY_SQL_8 =
      DefaultDialect.builder("MY_SQL_8")
          .fetchNextInAllTopics(
              "WITH raw AS(SELECT {{allFields}}, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq)) as rn"
                  + " FROM {{table}} WHERE processed = false AND topic <> '*')"
                  + " SELECT * FROM raw WHERE rn = 1 AND nextAttemptTime < ? LIMIT {{batchSize}}")
          .deleteExpired(
              "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false"
                  + " LIMIT {{batchSize}}")
          .selectBatch(
              "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
                  + "AND blocked = false AND processed = false AND topic = '*' LIMIT {{batchSize}} FOR UPDATE "
                  + "SKIP LOCKED")
          .lock(
              "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR "
                  + "UPDATE SKIP LOCKED")
          .build();
  Dialect POSTGRESQL_9 =
      DefaultDialect.builder("POSTGRESQL_9")
          .fetchNextInAllTopics(
              "WITH raw AS(SELECT {{allFields}}, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq)) as rn"
                  + " FROM {{table}} WHERE processed = false AND topic <> '*')"
                  + " SELECT * FROM raw WHERE rn = 1 AND nextAttemptTime < ? LIMIT {{batchSize}}")
          .deleteExpired(
              "DELETE FROM {{table}} WHERE id IN "
                  + "(SELECT id FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT {{batchSize}})")
          .selectBatch(
              "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
                  + "AND blocked = false AND processed = false AND topic = '*' LIMIT "
                  + "{{batchSize}} FOR UPDATE SKIP LOCKED")
          .lock(
              "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR "
                  + "UPDATE SKIP LOCKED")
          .changeMigration(
              5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)")
          .changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")
          .changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6)")
          .disableMigration(8)
          .build();

  Dialect H2 =
      DefaultDialect.builder("H2")
          .changeMigration(5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)")
          .changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")
          .disableMigration(8)
          .build();
  Dialect ORACLE =
      DefaultDialect.builder("ORACLE")
          .fetchNextInAllTopics(
              "WITH cte1 AS (SELECT {{allFields}}, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq)) as rn"
                  + " FROM {{table}} WHERE processed = 0 AND topic <> '*')"
                  + " SELECT * FROM cte1 WHERE rn = 1 AND nextAttemptTime < ? AND ROWNUM <= {{batchSize}}")
          .deleteExpired(
              "DELETE FROM {{table}} WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0 "
                  + "AND ROWNUM <= {{batchSize}}")
          .selectBatch(
              "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
                  + "AND blocked = 0 AND processed = 0 AND topic = '*' AND ROWNUM <= {{batchSize}} FOR UPDATE "
                  + "SKIP LOCKED")
          .lock(
              "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR "
                  + "UPDATE SKIP LOCKED")
          .checkSql("SELECT 1 FROM DUAL")
          .changeMigration(
              1,
              "CREATE TABLE TXNO_OUTBOX (\n"
                  + "    id VARCHAR2(36) PRIMARY KEY,\n"
                  + "    invocation CLOB,\n"
                  + "    nextAttemptTime TIMESTAMP(6),\n"
                  + "    attempts NUMBER,\n"
                  + "    blacklisted NUMBER(1),\n"
                  + "    version NUMBER\n"
                  + ")")
          .changeMigration(
              2, "ALTER TABLE TXNO_OUTBOX ADD uniqueRequestId VARCHAR(100) NULL UNIQUE")
          .changeMigration(3, "ALTER TABLE TXNO_OUTBOX ADD processed NUMBER(1)")
          .changeMigration(5, "ALTER TABLE TXNO_OUTBOX MODIFY uniqueRequestId VARCHAR2(250)")
          .changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")
          .changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD lastAttemptTime TIMESTAMP(6)")
          .disableMigration(8)
          .changeMigration(9, "ALTER TABLE TXNO_OUTBOX ADD topic VARCHAR(250) DEFAULT '*' NOT NULL")
          .changeMigration(10, "ALTER TABLE TXNO_OUTBOX ADD seq NUMBER")
          .changeMigration(
              11,
              "CREATE TABLE TXNO_SEQUENCE (topic VARCHAR(250) NOT NULL, seq NUMBER NOT NULL, CONSTRAINT PK_TXNO_SEQUENCE PRIMARY KEY (topic, seq))")
          .booleanValueFrom(v -> v ? "1" : "0")
          .createVersionTableBy(
              connection -> {
                try (Statement s = connection.createStatement()) {
                  try {
                    s.execute("CREATE TABLE TXNO_VERSION (version NUMBER)");
                  } catch (SQLException e) {
                    // oracle code for name already used by an existing object
                    if (!e.getMessage().contains("955")) {
                      throw e;
                    }
                  }
                }
              })
          .build();

  Dialect MS_SQL_SERVER = DefaultDialect.builder("MS_SQL_SERVER")
      .lock("SELECT id, invocation FROM {{table}} WITH (UPDLOCK, ROWLOCK, READPAST) WHERE id = ? AND version = ?")
      .selectBatch("SELECT TOP ({{batchSize}}) {{allFields}} FROM {{table}} " +
          "WITH (UPDLOCK, ROWLOCK, READPAST) WHERE nextAttemptTime < ? AND topic = '*' " +
          "AND blocked = 0 AND processed = 0")
      .deleteExpired("DELETE  TOP ({{batchSize}}) FROM {{table}} " +
          "WHERE nextAttemptTime < ? AND processed = 1 AND blocked = 0")
      .fetchCurrentVersion("SELECT version FROM TXNO_VERSION WITH (UPDLOCK, ROWLOCK, READPAST)")
      .fetchNextInAllTopics("SELECT TOP {{batchSize}} {{allFields}} FROM {{table}} a"
          + " WHERE processed = 0 AND topic <> '*' AND nextAttemptTime < ?"
          + " AND seq = ("
          + "SELECT MIN(seq) FROM {{table}} b WHERE b.topic=a.topic AND b.processed = 0"
          + ")")
      .booleanValueFrom(v -> v ? "1" : "0")
      .changeMigration(
          1,
          "CREATE TABLE TXNO_OUTBOX (\n"
              + "    id VARCHAR(36) PRIMARY KEY,\n"
              + "    invocation NVARCHAR(MAX),\n"
              + "    nextAttemptTime DATETIME2(6),\n"
              + "    attempts INT,\n"
              + "    blacklisted BIT,\n"
              + "    version INT\n"
              + ")")
      .changeMigration(
          2, "ALTER TABLE TXNO_OUTBOX ADD uniqueRequestId VARCHAR(100)")
      .changeMigration(3, "ALTER TABLE TXNO_OUTBOX ADD processed BIT")
      .changeMigration(5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId VARCHAR(250)")
      .changeMigration(6, "EXEC sp_rename 'TXNO_OUTBOX.blacklisted', 'blocked', 'COLUMN'")
      .changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD lastAttemptTime DATETIME2(6)")
      .changeMigration(8, "CREATE UNIQUE INDEX UX_TXNO_OUTBOX_uniqueRequestId ON TXNO_OUTBOX (uniqueRequestId) WHERE uniqueRequestId IS NOT NULL")
      .changeMigration(9, "ALTER TABLE TXNO_OUTBOX ADD topic VARCHAR(250) DEFAULT '*' NOT NULL")
      .changeMigration(10, "ALTER TABLE TXNO_OUTBOX ADD seq INT")
      .changeMigration(
          11,
          "CREATE TABLE TXNO_SEQUENCE (topic VARCHAR(250) NOT NULL, seq INT NOT NULL, CONSTRAINT " +
              "PK_TXNO_SEQUENCE PRIMARY KEY (topic, seq))")
      .createVersionTableBy(
          connection -> {
            try (Statement s = connection.createStatement()) {
              s.execute("IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TXNO_VERSION')\n"
                  + "BEGIN\n"
                  + "    CREATE TABLE TXNO_VERSION (\n"
                  + "        version INT\n"
                  + "    );"
                  + "END");
            }
          })
      .build();
}
