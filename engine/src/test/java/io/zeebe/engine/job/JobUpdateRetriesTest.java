/*
 * Zeebe Workflow Engine
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.engine.job;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.exporter.api.record.Assertions;
import io.zeebe.exporter.api.record.Record;
import io.zeebe.exporter.api.record.value.JobBatchRecordValue;
import io.zeebe.exporter.api.record.value.JobRecordValue;
import io.zeebe.protocol.RecordType;
import io.zeebe.protocol.RejectionType;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.test.util.record.RecordingExporter;
import org.junit.Rule;
import org.junit.Test;

public class JobUpdateRetriesTest {
  private static final String JOB_TYPE = "foo";
  private static final int NEW_RETRIES = 20;
  private static final String PROCESS_ID = "process";

  @Rule public EngineRule engineRule = new EngineRule();

  @Test
  public void shouldUpdateRetries() {
    // given
    engineRule.createJob(JOB_TYPE, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord =
        engineRule.jobs().withType(JOB_TYPE).activateAndWait();
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    engineRule.job().withRetries(0).fail(jobKey);

    // when
    final Record<JobRecordValue> updatedRecord =
        engineRule.job().withRetries(NEW_RETRIES).updateRetriesAndWait(jobKey);

    // then
    Assertions.assertThat(updatedRecord.getMetadata())
        .hasRecordType(RecordType.EVENT)
        .hasIntent(JobIntent.RETRIES_UPDATED);
    assertThat(updatedRecord.getKey()).isEqualTo(jobKey);

    Assertions.assertThat(updatedRecord.getValue())
        .hasWorker(job.getWorker())
        .hasType(job.getType())
        .hasRetries(NEW_RETRIES)
        .hasDeadline(job.getDeadline());
  }

  @Test
  public void shouldRejectUpdateRetriesIfJobNotFound() {
    // when
    final long position = engineRule.job().withRetries(NEW_RETRIES).updateRetries(123);
    final Record<JobRecordValue> rejection =
        RecordingExporter.jobRecords().withRecordType(RecordType.COMMAND_REJECTION).getFirst();

    // then
    assertThat(rejection.getMetadata().getIntent()).isEqualTo(JobIntent.UPDATE_RETRIES);
    assertThat(rejection.getMetadata().getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectUpdateRetriesIfJobCompleted() {
    // given
    engineRule.createJob(JOB_TYPE, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord =
        engineRule.jobs().withType(JOB_TYPE).activateAndWait();

    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    engineRule.job().withVariables("{}").complete(jobKey);

    // when
    engineRule.job().withRetries(NEW_RETRIES).updateRetries(jobKey);

    // then
    final Record<JobRecordValue> rejection =
        RecordingExporter.jobRecords().withRecordType(RecordType.COMMAND_REJECTION).getFirst();
    assertThat(rejection.getMetadata().getIntent()).isEqualTo(JobIntent.UPDATE_RETRIES);
    assertThat(rejection.getMetadata().getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldUpdateRetriesIfJobActivated() {
    // given
    engineRule.createJob(JOB_TYPE, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord =
        engineRule.jobs().withType(JOB_TYPE).activateAndWait();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    // when
    final Record<JobRecordValue> response =
        engineRule.job().withRetries(NEW_RETRIES).updateRetriesAndWait(jobKey);

    // then
    assertThat(response.getMetadata().getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getMetadata().getIntent()).isEqualTo(JobIntent.RETRIES_UPDATED);
    assertThat(response.getKey()).isEqualTo(jobKey);
    assertThat(response.getValue().getRetries()).isEqualTo(NEW_RETRIES);
  }

  @Test
  public void shouldUpdateRetriesIfJobCreated() {
    // given
    final long jobKey = engineRule.createJob(JOB_TYPE, PROCESS_ID).getKey();

    // when
    final Record<JobRecordValue> response =
        engineRule.job().withRetries(NEW_RETRIES).updateRetriesAndWait(jobKey);

    // then
    assertThat(response.getMetadata().getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(response.getMetadata().getIntent()).isEqualTo(JobIntent.RETRIES_UPDATED);
    assertThat(response.getKey()).isEqualTo(jobKey);
    assertThat(response.getValue().getRetries()).isEqualTo(NEW_RETRIES);
  }

  @Test
  public void shouldRejectUpdateRetriesIfRetriesZero() {
    // given
    engineRule.createJob(JOB_TYPE, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord =
        engineRule.jobs().withType(JOB_TYPE).activateAndWait();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    engineRule.job().withRetries(0).fail(jobKey);

    // when
    engineRule.job().withRetries(0).updateRetries(jobKey);
    final Record<JobRecordValue> rejection =
        RecordingExporter.jobRecords().withRecordType(RecordType.COMMAND_REJECTION).getFirst();

    // then
    assertThat(rejection.getMetadata().getIntent()).isEqualTo(JobIntent.UPDATE_RETRIES);
    assertThat(rejection.getMetadata().getRejectionType())
        .isEqualTo(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldRejectUpdateRetriesIfRetriesLessThanZero() {
    // given
    engineRule.createJob(JOB_TYPE, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord =
        engineRule.jobs().withType(JOB_TYPE).activateAndWait();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    engineRule.job().withRetries(0).fail(jobKey);

    // when
    engineRule.job().withRetries(-1).updateRetries(jobKey);
    final Record<JobRecordValue> rejection =
        RecordingExporter.jobRecords().withRecordType(RecordType.COMMAND_REJECTION).getFirst();

    // then
    assertThat(rejection.getMetadata().getIntent()).isEqualTo(JobIntent.UPDATE_RETRIES);
    assertThat(rejection.getMetadata().getRejectionType())
        .isEqualTo(RejectionType.INVALID_ARGUMENT);
  }
}
