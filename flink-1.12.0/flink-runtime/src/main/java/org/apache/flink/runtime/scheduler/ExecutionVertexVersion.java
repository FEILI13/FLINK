/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

/**
 * This class helps to record version of an execution vertex.
 */
public class ExecutionVertexVersion {

	private final ExecutionVertexID executionVertexId;

	private  long version;

	ExecutionVertexVersion(final ExecutionVertexID executionVertexId, final long version) {
		this.executionVertexId = executionVertexId;
		this.version = version;
	}

	public ExecutionVertexID getExecutionVertexId() {
		return executionVertexId;
	}

	public long getVersion() {
		return version;
	}

	public void add(){
		version++;
	}
}
