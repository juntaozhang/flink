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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.nodes.common.CommonIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecProcessTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A generator that generates a {@link ExecNode} graph from a graph of {@link FlinkPhysicalRel}s.
 *
 * <p>This traverses the tree of {@link FlinkPhysicalRel} starting from the sinks. At each rel we
 * recursively transform the inputs, then create a {@link ExecNode}. Each rel will be visited only
 * once, that means a rel will only generate one ExecNode instance.
 *
 * <p>Exchange and Union will create a actual node in the {@link ExecNode} graph as the first step,
 * once all ExecNodes' implementation are separated from physical rel, we will use {@link
 * InputProperty} to replace them.
 */
public class ExecNodeGraphGenerator {

    private final Map<FlinkPhysicalRel, ExecNode<?>> visitedRels;
    private final Set<String> visitedProcessTableFunctionUids;

    public ExecNodeGraphGenerator() {
        this.visitedRels = new IdentityHashMap<>();
        this.visitedProcessTableFunctionUids = new HashSet<>();
    }

    public ExecNodeGraph generate(List<FlinkPhysicalRel> relNodes, boolean isCompiled) {
        List<ExecNode<?>> rootNodes = new ArrayList<>(relNodes.size());
        for (FlinkPhysicalRel relNode : relNodes) {
            rootNodes.add(generate(relNode, isCompiled));
        }
        return new ExecNodeGraph(rootNodes);
    }

    private ExecNode<?> generate(FlinkPhysicalRel rel, boolean isCompiled) {
        ExecNode<?> execNode = visitedRels.get(rel);
        if (execNode != null) {
            return execNode;
        }

        if (rel instanceof CommonIntermediateTableScan) {
            throw new TableException("Intermediate RelNode can't be converted to ExecNode.");
        }

        List<ExecNode<?>> inputNodes = new ArrayList<>();
        for (RelNode input : rel.getInputs()) {
            inputNodes.add(generate((FlinkPhysicalRel) input, isCompiled));
        }

        execNode = rel.translateToExecNode(isCompiled);
        // connects the input nodes
        List<ExecEdge> inputEdges = new ArrayList<>(inputNodes.size());
        for (ExecNode<?> inputNode : inputNodes) {
            inputEdges.add(ExecEdge.builder().source(inputNode).target(execNode).build());
        }
        execNode.setInputEdges(inputEdges);
        checkUidForProcessTableFunction(execNode);
        visitedRels.put(rel, execNode);
        return execNode;
    }

    private void checkUidForProcessTableFunction(ExecNode<?> execNode) {
        if (!(execNode instanceof StreamExecProcessTableFunction)) {
            return;
        }
        final String uid = ((StreamExecProcessTableFunction) execNode).getUid();
        if (uid == null) {
            return;
        }
        if (visitedProcessTableFunctionUids.contains(uid)) {
            throw new ValidationException(
                    String.format(
                            "Duplicate unique identifier '%s' detected among process table functions. "
                                    + "Make sure that all PTF calls have an identifier defined that is globally unique. "
                                    + "Please provide a custom identifier using the implicit `uid` argument. "
                                    + "For example: myFunction(..., uid => 'my-id')",
                            uid));
        }
        visitedProcessTableFunctionUids.add(uid);
    }
}
