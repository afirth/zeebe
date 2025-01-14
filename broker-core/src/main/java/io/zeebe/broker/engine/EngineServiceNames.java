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
package io.zeebe.broker.engine;

import io.zeebe.engine.processor.workflow.message.command.SubscriptionCommandMessageHandler;
import io.zeebe.engine.state.StateStorageFactory;
import io.zeebe.logstreams.impl.delete.DeletionService;
import io.zeebe.servicecontainer.ServiceName;

public class EngineServiceNames {
  public static final ServiceName<StateStorageFactory> stateStorageFactoryServiceName(
      String partitionName) {
    return ServiceName.newServiceName(
        String.format("%s.rocksdb.storage", partitionName), StateStorageFactory.class);
  }

  public static final ServiceName<EngineService> ENGINE_SERVICE_NAME =
      ServiceName.newServiceName("logstreams.processor", EngineService.class);

  public static final ServiceName<SubscriptionCommandMessageHandler>
      SUBSCRIPTION_API_MESSAGE_HANDLER_SERVICE_NAME =
          ServiceName.newServiceName(
              "broker.subscriptionApi.messageHandler", SubscriptionCommandMessageHandler.class);

  public static final ServiceName<DeletionService> leaderLogStreamDeletionService(int partitionId) {
    return ServiceName.newServiceName(
        String.format("logstream.%d.deletion", partitionId), DeletionService.class);
  }
}
