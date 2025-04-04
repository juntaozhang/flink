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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.planner.calcite.RexTableArgCall
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWindowTableFunction
import org.apache.flink.table.planner.plan.utils.WindowUtil
import org.apache.flink.table.planner.plan.utils.WindowUtil.{convertToWindowingStrategy, validateTimeFieldWithTimeAttribute}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rex.RexCall

/**
 * Rule to convert a [[FlinkLogicalTableFunctionScan]] with window table function call into a
 * [[StreamPhysicalWindowTableFunction]].
 */
class StreamPhysicalWindowTableFunctionRule(config: Config) extends ConverterRule(config) {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: FlinkLogicalTableFunctionScan = call.rel(0)
    WindowUtil.isWindowTableFunctionCall(scan.getCall)
  }

  def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalTableFunctionScan = rel.asInstanceOf[FlinkLogicalTableFunctionScan]

    val requiredDistribution = getDistribution(scan.getCall.asInstanceOf[RexCall])
    val requiredTraitSet = rel.getCluster.getPlanner
      .emptyTraitSet()
      .replace(requiredDistribution)
      .replace(FlinkConventions.STREAM_PHYSICAL)
    val providedTraitSet = rel.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput = RelOptRule.convert(scan.getInput(0), requiredTraitSet)

    val windowTableFunction = scan.getCall.asInstanceOf[RexCall]
    val inputRowType = newInput.getRowType
    // Time field of window table function in streaming mode should be with time attribute
    validateTimeFieldWithTimeAttribute(windowTableFunction, inputRowType)
    new StreamPhysicalWindowTableFunction(
      scan.getCluster,
      providedTraitSet,
      newInput,
      scan.getRowType,
      convertToWindowingStrategy(windowTableFunction, newInput)
    )
  }

  private def getDistribution(windowCall: RexCall): FlinkRelDistribution = {
    windowCall.getOperator match {
      case FlinkSqlOperatorTable.SESSION =>
        val tableArgCall = windowCall.operands.get(0).asInstanceOf[RexTableArgCall]
        val partitionKeys = tableArgCall.getPartitionKeys
        if (partitionKeys.nonEmpty) {
          FlinkRelDistribution.hash(partitionKeys, requireStrict = true)
        } else {
          FlinkRelDistribution.SINGLETON
        }
      case _ =>
        FlinkRelDistribution.DEFAULT
    }
  }
}

object StreamPhysicalWindowTableFunctionRule {
  val INSTANCE = new StreamPhysicalWindowTableFunctionRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalTableFunctionScan],
      FlinkConventions.LOGICAL,
      FlinkConventions.STREAM_PHYSICAL,
      "StreamPhysicalWindowTableFunctionRule"))
}
