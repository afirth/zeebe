/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.distributedlog.restore;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.rocksdb.ZeebeRocksDbFactory;
import io.zeebe.distributedlog.impl.LogstreamConfig;
import io.zeebe.distributedlog.restore.RestoreInfoResponse.ReplicationTarget;
import io.zeebe.distributedlog.restore.impl.ControllableSnapshotRestoreContext;
import io.zeebe.distributedlog.restore.impl.DefaultRestoreInfoResponse;
import io.zeebe.distributedlog.restore.impl.ReplicatingRestoreClient;
import io.zeebe.distributedlog.restore.impl.ReplicatingRestoreClientProvider;
import io.zeebe.distributedlog.restore.impl.RestoreController;
import io.zeebe.distributedlog.restore.log.LogReplicator;
import io.zeebe.distributedlog.restore.snapshot.RestoreSnapshotReplicator;
import io.zeebe.distributedlog.restore.snapshot.impl.SnapshotConsumerImpl;
import io.zeebe.logstreams.state.StateSnapshotController;
import io.zeebe.logstreams.state.StateStorage;
import io.zeebe.logstreams.util.LogStreamReaderRule;
import io.zeebe.logstreams.util.LogStreamRule;
import io.zeebe.logstreams.util.LogStreamWriterRule;
import io.zeebe.logstreams.util.RocksDBWrapper;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestoreControllerTest {

  private static final int VALUE = 0xCAFE;
  private static final String KEY = "test";
  private static final DirectBuffer EVENT = wrapString("FOO");

  public TemporaryFolder temporaryFolderClient = new TemporaryFolder();
  public LogStreamRule logStreamRuleClient = new LogStreamRule(temporaryFolderClient);
  public LogStreamWriterRule writerClient = new LogStreamWriterRule(logStreamRuleClient);
  public LogStreamReaderRule readerClient = new LogStreamReaderRule(logStreamRuleClient);

  public TemporaryFolder temporaryFolderServer = new TemporaryFolder();
  public LogStreamRule logStreamRuleServer = new LogStreamRule(temporaryFolderServer);
  public LogStreamWriterRule writerServer = new LogStreamWriterRule(logStreamRuleServer);
  public LogStreamReaderRule readerServer = new LogStreamReaderRule(logStreamRuleServer);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(temporaryFolderClient)
          .around(logStreamRuleClient)
          .around(readerClient)
          .around(writerClient)
          .around(temporaryFolderServer)
          .around(logStreamRuleServer)
          .around(readerServer)
          .around(writerServer);

  private ControllableSnapshotRestoreContext snapshotRestoreContext;

  private StateSnapshotController replicatorSnapshotController;
  private StateStorage receiverStorage;
  private ReplicatingRestoreClient restoreClient;
  private String clientNodeId = "0";
  private RestoreController restoreController;

  @Before
  public void setUp() throws IOException {
    final File runtimeDirectory = temporaryFolderServer.newFolder("runtime");
    final File snapshotsDirectory = temporaryFolderServer.newFolder("snapshots");
    final StateStorage storage = new StateStorage(runtimeDirectory, snapshotsDirectory);
    replicatorSnapshotController =
        new StateSnapshotController(
            ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class), storage, null, 1);
    final RocksDBWrapper wrapper = new RocksDBWrapper();
    wrapper.wrap(replicatorSnapshotController.openDb());
    wrapper.putInt(KEY, VALUE);

    final File receiverRuntimeDirectory = temporaryFolderClient.newFolder("runtime-receiver");
    final File receiverSnapshotsDirectory = temporaryFolderClient.newFolder("snapshots-receiver");
    receiverStorage = new StateStorage(receiverRuntimeDirectory, receiverSnapshotsDirectory);
    snapshotRestoreContext = new ControllableSnapshotRestoreContext();
    snapshotRestoreContext.setProcessorStateStorage(receiverStorage);

    restoreClient =
        new ReplicatingRestoreClient(
            replicatorSnapshotController, logStreamRuleServer.getLogStream());
    restoreController = createRestoreController();
  }

  private RestoreController createRestoreController() {
    final ReplicatingRestoreClientProvider provider =
        new ReplicatingRestoreClientProvider(restoreClient, snapshotRestoreContext);
    LogstreamConfig.putRestoreFactory(clientNodeId, provider);

    final RestoreNodeProvider nodeProvider = provider.createNodeProvider(1);
    final ThreadContext restoreThreadContext = new SingleThreadContext("test");
    final Logger log = LoggerFactory.getLogger("test");

    final LogReplicator logReplicator =
        new LogReplicator(
            (commitPosition, blockBuffer) -> {
              logStreamRuleClient.getLogStream().setCommitPosition(commitPosition);
              return logStreamRuleClient
                  .getLogStream()
                  .getLogStorage()
                  .append(ByteBuffer.wrap(blockBuffer));
            },
            restoreClient,
            restoreThreadContext,
            log);

    final RestoreSnapshotReplicator snapshotReplicator =
        new RestoreSnapshotReplicator(
            restoreClient,
            snapshotRestoreContext,
            new SnapshotConsumerImpl(receiverStorage, log),
            receiverStorage,
            1,
            restoreThreadContext,
            log);
    return new RestoreController(
        restoreClient, nodeProvider, logReplicator, snapshotReplicator, restoreThreadContext, log);
  }

  @Test
  public void shouldRestoreFromLogEvents() {
    // given
    final int numEventsInBackup = 10;
    final int numExtraEvents = 5;

    final long backupPosition = writerServer.writeEvents(numEventsInBackup, EVENT);
    writerServer.writeEvents(numExtraEvents, EVENT);

    // when
    final long restoredPosition = restoreController.restore(-1, backupPosition);

    // then
    assertThat(restoredPosition).isEqualTo(backupPosition);
    readerClient.assertEvents(numEventsInBackup, EVENT);
  }

  @Test
  public void shouldRestoreFromSnapshotAtBackUpPosition() {
    final int numEventsInSnapshot = 10;

    final long snapshotPosition = writerServer.writeEvents(numEventsInSnapshot, EVENT);
    replicatorSnapshotController.takeSnapshot(snapshotPosition);

    snapshotRestoreContext.setProcessorPositionSupplier(() -> snapshotPosition);
    snapshotRestoreContext.setExporterPositionSupplier(() -> snapshotPosition);

    final long restoredPosition = restoreController.restore(-1, snapshotPosition);

    assertThat(restoredPosition).isEqualTo(snapshotPosition);
    assertThat(readerClient.readEvents().size()).isEqualTo(1);
    assertThat(
            Arrays.stream(receiverStorage.getSnapshotsDirectory().listFiles())
                .anyMatch(f -> f.getName().equals(String.valueOf(snapshotPosition))))
        .isTrue();
  }

  @Test
  public void shouldRestoreFromSnapshotLessThanBackUpPosition() {

    // given
    final int numEventsInSnapshot = 5;
    final int numEventsAfterSnapshotInBackup = 10;

    final long snapshotPosition = writerServer.writeEvents(numEventsInSnapshot, EVENT);
    replicatorSnapshotController.takeSnapshot(snapshotPosition);
    final long backupPosition = writerServer.writeEvents(numEventsAfterSnapshotInBackup, EVENT);
    snapshotRestoreContext.setProcessorPositionSupplier(() -> snapshotPosition);
    snapshotRestoreContext.setExporterPositionSupplier(() -> snapshotPosition);

    // when
    final long restoredPosition = restoreController.restore(-1, backupPosition);

    // then
    assertThat(restoredPosition).isEqualTo(backupPosition);
    assertThat(readerClient.readEvents().size()).isEqualTo(numEventsAfterSnapshotInBackup + 1);
    assertThat(
            Arrays.stream(receiverStorage.getSnapshotsDirectory().listFiles())
                .anyMatch(f -> f.getName().equals(String.valueOf(snapshotPosition))))
        .isTrue();
  }

  @Test
  public void shouldRestoreFromSnapshotGreaterThanBackUpPosition() {
    // given
    final int numEventsBeforeBackUp = 10;
    final int numEventsInSnapshotAfterBackup = 5;

    final long backupPosition = writerServer.writeEvents(numEventsBeforeBackUp, EVENT);
    final long snapshotPosition = writerServer.writeEvents(numEventsInSnapshotAfterBackup, EVENT);
    replicatorSnapshotController.takeSnapshot(snapshotPosition);
    snapshotRestoreContext.setProcessorPositionSupplier(() -> snapshotPosition);
    snapshotRestoreContext.setExporterPositionSupplier(() -> snapshotPosition);

    // when
    final long restoredPosition = restoreController.restore(-1, backupPosition);

    // then
    assertThat(restoredPosition).isEqualTo(snapshotPosition);
    assertThat(readerClient.readEvents().size()).isEqualTo(1);
    assertThat(
            Arrays.stream(receiverStorage.getSnapshotsDirectory().listFiles())
                .anyMatch(f -> f.getName().equals(String.valueOf(snapshotPosition))))
        .isTrue();
  }

  @Test
  public void shouldRestoreFromLatestExportedPosition() {
    // given
    final int numEventsExported = 5;
    final int numEventsNotExported = 10;

    final long exporterPosition = writerServer.writeEvents(numEventsExported, EVENT);
    final long backupPosition = writerServer.writeEvents(numEventsNotExported, EVENT);
    replicatorSnapshotController.takeSnapshot(backupPosition);
    snapshotRestoreContext.setProcessorPositionSupplier(() -> backupPosition);
    snapshotRestoreContext.setExporterPositionSupplier(() -> exporterPosition);

    // when
    restoreController.restore(-1, backupPosition);

    // then
    assertThat(readerClient.readEvents().size()).isEqualTo(1 + numEventsNotExported);
  }

  @Test
  public void shouldRestoreEventsBetweenExporterAndSnapshotPosition() {
    // given
    final int numEventsExported = 5;
    final int numEventsNotExportedInBackup = 10;
    final int numEventsNotExportedAfterBackup = 2;

    final long exporterPosition = writerServer.writeEvents(numEventsExported, EVENT);
    final long backupPosition = writerServer.writeEvents(numEventsNotExportedInBackup, EVENT);
    final long snapshotPosition = writerServer.writeEvents(numEventsNotExportedAfterBackup, EVENT);
    replicatorSnapshotController.takeSnapshot(snapshotPosition);
    snapshotRestoreContext.setProcessorPositionSupplier(() -> snapshotPosition);
    snapshotRestoreContext.setExporterPositionSupplier(() -> exporterPosition);

    // when
    restoreController.restore(-1, backupPosition);

    readerServer.readEvents();
    // then
    assertThat(readerClient.readEvents().size())
        .isEqualTo(1 + numEventsNotExportedInBackup + numEventsNotExportedAfterBackup);
  }

  @Test
  public void shouldThrowExceptionIfSnapshotReplicationFailed() {
    restoreClient.setFailSnapshotChunk(true);

    final long backupPosition = writerServer.writeEvents(10, EVENT);
    replicatorSnapshotController.takeSnapshot(backupPosition);

    assertThatThrownBy(() -> restoreController.restore(-1, 10)).isNotNull();
  }

  @Test
  public void shouldThrowExceptionIfNoneStrategy() {
    final DefaultRestoreInfoResponse defaultRestoreInfoResponse = new DefaultRestoreInfoResponse();
    defaultRestoreInfoResponse.setReplicationTarget(ReplicationTarget.NONE);
    restoreClient.completeRestoreInfoResponse(defaultRestoreInfoResponse);

    assertThatThrownBy(() -> restoreController.restore(-1, 10)).isNotNull();
  }

  @Test
  public void shouldThrowExceptionIfRequestInfoFailed() {
    restoreClient.completeRestoreInfoResponse(new RuntimeException());
    assertThatThrownBy(() -> restoreController.restore(-1, 10)).isNotNull();
  }
}
