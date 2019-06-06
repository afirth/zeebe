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
package io.zeebe.distributedlog.restore.impl;

import io.atomix.cluster.MemberId;
import io.zeebe.distributedlog.restore.RestoreClient;
import io.zeebe.distributedlog.restore.RestoreInfoRequest;
import io.zeebe.distributedlog.restore.RestoreInfoResponse;
import io.zeebe.distributedlog.restore.log.LogReplicationRequest;
import io.zeebe.distributedlog.restore.log.LogReplicationResponse;
import io.zeebe.distributedlog.restore.log.impl.DefaultLogReplicationRequestHandler;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreRequest;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreResponse;
import io.zeebe.distributedlog.restore.snapshot.impl.DefaultSnapshotRequestHandler;
import io.zeebe.distributedlog.restore.snapshot.impl.DefaultSnapshotRestoreResponse;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.state.StateSnapshotController;
import java.util.concurrent.CompletableFuture;

public class ReplicatingRestoreClient implements RestoreClient {

  private final StateSnapshotController replicatorSnapshotController;
  private final LogStream serverLogstream;
  private CompletableFuture<RestoreInfoResponse> restoreInfoResponse = new CompletableFuture<>();
  private boolean autoResponse = true;
  private boolean failSnapshotChunk = false;

  public ReplicatingRestoreClient(
      StateSnapshotController serverSnapshotController, LogStream serverLogstream) {
    this.replicatorSnapshotController = serverSnapshotController;
    this.serverLogstream = serverLogstream;
  }

  public void setFailSnapshotChunk(boolean fail) {
    this.failSnapshotChunk = fail;
  }

  @Override
  public CompletableFuture<SnapshotRestoreResponse> requestSnapshotChunk(
      MemberId server, SnapshotRestoreRequest request) {
    if (failSnapshotChunk) {
      return CompletableFuture.completedFuture(new DefaultSnapshotRestoreResponse());
    }
    return CompletableFuture.completedFuture(
        new DefaultSnapshotRequestHandler(replicatorSnapshotController).onSnapshotRequest(request));
  }

  @Override
  public CompletableFuture<LogReplicationResponse> requestLogReplication(
      MemberId server, LogReplicationRequest request) {
    return CompletableFuture.completedFuture(
        new DefaultLogReplicationRequestHandler(serverLogstream).onReplicationRequest(request));
  }

  @Override
  public CompletableFuture<RestoreInfoResponse> requestRestoreInfo(
      MemberId server, RestoreInfoRequest request) {
    if (autoResponse) {
      return CompletableFuture.completedFuture(
          new DefaultRestoreInfoRequestHandler(serverLogstream, replicatorSnapshotController)
              .onRestoreInfoRequest(request));
    }
    return restoreInfoResponse;
  }

  public void completeRestoreInfoResponse(RestoreInfoResponse defaultRestoreInfoResponse) {
    autoResponse = false;
    restoreInfoResponse.complete(defaultRestoreInfoResponse);
  }

  public void completeRestoreInfoResponse(Throwable e) {
    autoResponse = false;
    restoreInfoResponse.completeExceptionally(e);
  }
}
