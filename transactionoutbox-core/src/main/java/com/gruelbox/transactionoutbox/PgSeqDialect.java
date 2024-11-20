package com.gruelbox.transactionoutbox;

public final class PgSeqDialect {
  public static Dialect POSTGRESQL_SEQ =
      DefaultDialect.builder("POSTGRESQL_SEQ")
          .fetchNextSequence("SELECT nextval('txno_outbox_seq_seq')")
          .fetchNextInAllTopics(
              "WITH raw AS(SELECT {{allFields}}, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq)) as rn"
                  + " FROM {{table}} WHERE processed = false AND topic <> '*')"
                  + " SELECT * FROM raw WHERE rn = 1 AND (blocked = false OR orderedTakeLast = false) AND nextAttemptTime < ? LIMIT {{batchSize}}")
          .deleteOutdatedInAllTopics(
              "WITH raw AS(SELECT id, (ROW_NUMBER() OVER(PARTITION BY topic ORDER BY seq DESC)) as rn"
                  + " FROM {{table}} WHERE orderedTakeLast = true AND topic <> '*')"
                  + " DELETE FROM {{table}} WHERE id IN (SELECT raw.id FROM raw WHERE rn > 1)")
          .deleteExpired(
              "DELETE FROM {{table}} WHERE id IN "
                  + "(SELECT id FROM {{table}} WHERE nextAttemptTime < ? AND processed = true AND blocked = false LIMIT {{batchSize}})")
          .selectBatch(
              "SELECT {{allFields}} FROM {{table}} WHERE nextAttemptTime < ? "
                  + "AND blocked = false AND processed = false AND topic = '*' ORDER BY seq LIMIT "
                  + "{{batchSize}} FOR UPDATE SKIP LOCKED")
          .lock(
              "SELECT id, invocation FROM {{table}} WHERE id = ? AND version = ? FOR "
                  + "UPDATE SKIP LOCKED")
          .changeMigration(
              5, "ALTER TABLE TXNO_OUTBOX ALTER COLUMN uniqueRequestId TYPE VARCHAR(250)")
          .changeMigration(6, "ALTER TABLE TXNO_OUTBOX RENAME COLUMN blacklisted TO blocked")
          .changeMigration(7, "ALTER TABLE TXNO_OUTBOX ADD COLUMN lastAttemptTime TIMESTAMP(6)")
          .disableMigration(8)
          .changeMigration(10, "ALTER TABLE TXNO_OUTBOX ADD COLUMN seq BIGSERIAL NOT NULL")
          .disableMigration(11)
          .build();

  private PgSeqDialect() {}
}
