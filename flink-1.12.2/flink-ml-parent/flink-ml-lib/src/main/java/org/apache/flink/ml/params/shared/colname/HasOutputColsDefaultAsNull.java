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

package org.apache.flink.ml.params.shared.colname;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.WithParams;

/**
 * An interface for classes with a parameter specifying names of multiple output columns. The
 * default parameter value is null.
 *
 * @see HasOutputCol
 * @see HasOutputColDefaultAsNull
 * @see HasOutputCols
 */
public interface HasOutputColsDefaultAsNull<T> extends WithParams<T> {

    ParamInfo<String[]> OUTPUT_COLS =
            ParamInfoFactory.createParamInfo("outputCols", String[].class)
                    .setDescription("Names of the output columns")
                    .setHasDefaultValue(null)
                    .build();

    default String[] getOutputCols() {
        return get(OUTPUT_COLS);
    }

    default T setOutputCols(String... value) {
        return set(OUTPUT_COLS, value);
    }
}
