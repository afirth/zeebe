/*
 * Zeebe Broker Core
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
package io.zeebe.broker.logstreams.restore;

import io.zeebe.broker.engine.impl.SnapshotChunkImpl;
import io.zeebe.clustering.management.BooleanType;
import io.zeebe.clustering.management.SnapshotRestoreResponseDecoder;
import io.zeebe.clustering.management.SnapshotRestoreResponseEncoder;
import io.zeebe.distributedlog.restore.snapshot.SnapshotRestoreResponse;
import io.zeebe.distributedlog.restore.snapshot.impl.DefaultSnapshotRestoreResponse;
import io.zeebe.engine.util.SbeBufferWriterReader;
import io.zeebe.logstreams.state.SnapshotChunk;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class SbeSnapshotRestoreResponse
    extends SbeBufferWriterReader<SnapshotRestoreResponseEncoder, SnapshotRestoreResponseDecoder>
    implements SnapshotRestoreResponse {

  private final SnapshotRestoreResponseEncoder encoder;
  private final SnapshotRestoreResponseDecoder decoder;

  private final DefaultSnapshotRestoreResponse delegate;
  private DirectBuffer snapshotChunkBuffer;

  public SbeSnapshotRestoreResponse() {
    delegate = new DefaultSnapshotRestoreResponse();
    snapshotChunkBuffer = new UnsafeBuffer(0, 0);
    encoder = new SnapshotRestoreResponseEncoder();
    decoder = new SnapshotRestoreResponseDecoder();
    reset();
  }

  public SbeSnapshotRestoreResponse(SnapshotRestoreResponse other) {
    this();
    setSuccess(other.isSuccess());
    delegate.setSnapshotChunk(other.getSnapshotChunk());
    if (isSuccess()) {
      final byte[] bytes = new SnapshotChunkImpl(other.getSnapshotChunk()).toBytes();
      snapshotChunkBuffer.wrap(bytes, 0, bytes.length);
    }
  }

  public SbeSnapshotRestoreResponse(byte[] bytes) {
    this();
    wrap(new UnsafeBuffer(bytes));
  }

  @Override
  public void wrap(DirectBuffer buffer, int offset, int length) {
    super.wrap(buffer, offset, length);
    final boolean valid = decoder.success() == BooleanType.TRUE;
    setSuccess(valid);
    if (valid) {
      decoder.wrapSnapshotChunk(snapshotChunkBuffer);
      setSnapshotChunk(snapshotChunkBuffer);
    }
  }

  private void setSnapshotChunk(DirectBuffer snapshotChunkBuffer) {
    final SnapshotChunkImpl snapshotChunk = new SnapshotChunkImpl();
    snapshotChunk.wrap(snapshotChunkBuffer);
    delegate.setSnapshotChunk(snapshotChunk);
  }

  private void setSuccess(boolean success) {
    delegate.setSuccess(success);
  }

  @Override
  public void write(MutableDirectBuffer buffer, int offset) {
    super.write(buffer, offset);
    encoder.success(delegate.isSuccess() ? BooleanType.TRUE : BooleanType.FALSE);
    encoder.putSnapshotChunk(snapshotChunkBuffer, 0, snapshotChunkBuffer.capacity());
  }

  @Override
  public SnapshotChunk getSnapshotChunk() {
    return delegate.getSnapshotChunk();
  }

  @Override
  public boolean isSuccess() {
    return delegate.isSuccess();
  }

  public static byte[] serialize(SnapshotRestoreResponse response) {
    return new SbeSnapshotRestoreResponse(response).toBytes();
  }

  @Override
  protected SnapshotRestoreResponseEncoder getBodyEncoder() {
    return encoder;
  }

  @Override
  protected SnapshotRestoreResponseDecoder getBodyDecoder() {
    return decoder;
  }

  @Override
  public int getLength() {
    return super.getLength()
        + SnapshotRestoreResponseEncoder.snapshotChunkHeaderLength()
        + snapshotChunkBuffer.capacity();
  }
}
