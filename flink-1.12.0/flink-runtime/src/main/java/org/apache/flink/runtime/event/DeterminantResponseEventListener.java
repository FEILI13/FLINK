/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.event;

import org.apache.flink.runtime.causal.DeterminantResponseEvent;
import org.apache.flink.runtime.causal.recovery.RecoveryManager;
import org.apache.flink.runtime.util.event.EventListener;

public class DeterminantResponseEventListener implements EventListener<TaskEvent> {

	private final ClassLoader userCodeClassLoader;
	private final RecoveryManager recoveryManager;
	private final Object checkpointLock;


	public DeterminantResponseEventListener(ClassLoader userCodeClassLoader, RecoveryManager recoveryManager,
                                            Object checkpointLock) {
		this.userCodeClassLoader = userCodeClassLoader;
		this.recoveryManager = recoveryManager;
		this.checkpointLock = checkpointLock;
	}

	@Override
	public void onEvent(TaskEvent event) {
		if (event instanceof DeterminantResponseEvent) {
			synchronized (checkpointLock) {
				recoveryManager.notifyDeterminantResponseEvent((DeterminantResponseEvent) event);
			}
		}
		else {
			throw new IllegalArgumentException(String.format("Unknown event type: %s.", event));
		}

	}
}
