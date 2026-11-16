/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.groupwindow.NamedWindowProperty;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based global aggregate. */
@ExecNodeMetadata(
    name = "stream-exec-global-window-aggregate",
    version = 1,
    consumedOptions = "table.local-time-zone",
    producedTransformations =
        StreamExecGlobalWindowAggregate.GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecGlobalWindowAggregate extends StreamExecWindowAggregateBase {

  public static final String GLOBAL_WINDOW_AGGREGATE_TRANSFORMATION = "global-window-aggregate";

  public static final String FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE = "localAggInputRowType";

  @JsonProperty(FIELD_NAME_GROUPING)
  private final int[] grouping;

  @JsonProperty(FIELD_NAME_AGG_CALLS)
  private final AggregateCall[] aggCalls;

  @JsonProperty(FIELD_NAME_WINDOWING)
  private final WindowingStrategy windowing;

  @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES)
  private final NamedWindowProperty[] namedWindowProperties;

  /** The input row type of this node's local agg. */
  @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE)
  private final RowType localAggInputRowType;

  @JsonProperty(FIELD_NAME_NEED_RETRACTION)
  private final boolean needRetraction;

  public StreamExecGlobalWindowAggregate(
      ReadableConfig tableConfig,
      int[] grouping,
      AggregateCall[] aggCalls,
      WindowingStrategy windowing,
      NamedWindowProperty[] namedWindowProperties,
      Boolean needRetraction,
      InputProperty inputProperty,
      RowType localAggInputRowType,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecGlobalWindowAggregate.class),
        ExecNodeContext.newPersistedConfig(StreamExecGlobalWindowAggregate.class, tableConfig),
        grouping,
        aggCalls,
        windowing,
        namedWindowProperties,
        needRetraction,
        Collections.singletonList(inputProperty),
        localAggInputRowType,
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecGlobalWindowAggregate(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
      @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
      @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
      @JsonProperty(FIELD_NAME_NAMED_WINDOW_PROPERTIES) NamedWindowProperty[] namedWindowProperties,
      @Nullable @JsonProperty(FIELD_NAME_NEED_RETRACTION) Boolean needRetraction,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_LOCAL_AGG_INPUT_ROW_TYPE) RowType localAggInputRowType,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    this.grouping = checkNotNull(grouping);
    this.aggCalls = checkNotNull(aggCalls);
    this.windowing = checkNotNull(windowing);
    this.namedWindowProperties = checkNotNull(namedWindowProperties);
    this.localAggInputRowType = localAggInputRowType;
    this.needRetraction = Optional.ofNullable(needRetraction).orElse(false);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    // TODO(L4): wire the two-stage local-global path (the global stage
    // consumes local partial rows with the trailing windowEnd BIGINT
    // column via addIntermediateResults, windowEndIndex = getSliceEnd());
    // rejected at translation time until then - the single-stage
    // StreamExecWindowAggregate path carries these queries.
    // Note: the Flink-side transformation must stay non-keyed even when
    // wired (group-agg PR #13558 lesson: a Flink state key selector
    // conflicts with the velox key-group routing at parallelism > 1).
    throw new TableException(
        "GFV does not support the two-stage (local-global) window TVF aggregation plan yet (L4)");
  }
}
