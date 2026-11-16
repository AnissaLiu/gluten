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
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.logical.WindowingStrategy;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.utils.AggregateInfoList;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.tools.RelBuilder;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Stream {@link ExecNode} for window table-valued based local aggregate. */
@ExecNodeMetadata(
    name = "stream-exec-local-window-aggregate",
    version = 1,
    consumedOptions = "table.local-time-zone",
    producedTransformations = StreamExecLocalWindowAggregate.LOCAL_WINDOW_AGGREGATE_TRANSFORMATION,
    minPlanVersion = FlinkVersion.v1_15,
    minStateVersion = FlinkVersion.v1_15)
public class StreamExecLocalWindowAggregate extends StreamExecWindowAggregateBase {

  public static final String LOCAL_WINDOW_AGGREGATE_TRANSFORMATION = "local-window-aggregate";

  private static final long WINDOW_AGG_MEMORY_RATIO = 100;

  public static final String FIELD_NAME_WINDOWING = "windowing";

  @JsonProperty(FIELD_NAME_GROUPING)
  private final int[] grouping;

  @JsonProperty(FIELD_NAME_AGG_CALLS)
  private final AggregateCall[] aggCalls;

  @JsonProperty(FIELD_NAME_WINDOWING)
  private final WindowingStrategy windowing;

  @JsonProperty(FIELD_NAME_NEED_RETRACTION)
  private final boolean needRetraction;

  public StreamExecLocalWindowAggregate(
      ReadableConfig tableConfig,
      int[] grouping,
      AggregateCall[] aggCalls,
      WindowingStrategy windowing,
      Boolean needRetraction,
      InputProperty inputProperty,
      RowType outputType,
      String description) {
    this(
        ExecNodeContext.newNodeId(),
        ExecNodeContext.newContext(StreamExecLocalWindowAggregate.class),
        ExecNodeContext.newPersistedConfig(StreamExecLocalWindowAggregate.class, tableConfig),
        grouping,
        aggCalls,
        windowing,
        needRetraction,
        Collections.singletonList(inputProperty),
        outputType,
        description);
  }

  @JsonCreator
  public StreamExecLocalWindowAggregate(
      @JsonProperty(FIELD_NAME_ID) int id,
      @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
      @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
      @JsonProperty(FIELD_NAME_GROUPING) int[] grouping,
      @JsonProperty(FIELD_NAME_AGG_CALLS) AggregateCall[] aggCalls,
      @JsonProperty(FIELD_NAME_WINDOWING) WindowingStrategy windowing,
      @Nullable @JsonProperty(FIELD_NAME_NEED_RETRACTION) Boolean needRetraction,
      @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
      @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
      @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
    super(id, context, persistedConfig, inputProperties, outputType, description);
    this.grouping = checkNotNull(grouping);
    this.aggCalls = checkNotNull(aggCalls);
    this.windowing = checkNotNull(windowing);
    this.needRetraction = Optional.ofNullable(needRetraction).orElse(false);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Transformation<RowData> translateToPlanInternal(
      PlannerBase planner, ExecNodeConfig config) {
    // TODO(L4): wire the two-stage local-global path (local partial rows
    // with the trailing windowEnd BIGINT column, consumed by the global
    // stage via addIntermediateResults); rejected at translation time
    // until then - the single-stage StreamExecWindowAggregate path carries
    // these queries.
    throw new TableException(
        "GFV does not support the two-stage (local-global) window TVF aggregation plan yet (L4)");
  }

  private GeneratedNamespaceAggsHandleFunction<Long> createAggsHandler(
      SliceAssigner sliceAssigner,
      AggregateInfoList aggInfoList,
      ExecNodeConfig config,
      ClassLoader classLoader,
      RelBuilder relBuilder,
      List<LogicalType> fieldTypes,
      ZoneId shiftTimeZone) {
    final AggsHandlerCodeGenerator generator =
        new AggsHandlerCodeGenerator(
                new CodeGeneratorContext(config, classLoader),
                relBuilder,
                JavaScalaConversionUtil.toScala(fieldTypes),
                true) // copyInputField
            .needAccumulate()
            .needMerge(0, true, null);

    if (needRetraction) {
      generator.needRetract();
    }

    return generator.generateNamespaceAggsHandler(
        "LocalWindowAggsHandler",
        aggInfoList,
        JavaScalaConversionUtil.toScala(Collections.emptyList()),
        sliceAssigner,
        // we use window end timestamp to indicate a slicing window, see SliceAssigner
        Long.class,
        shiftTimeZone);
  }
}
