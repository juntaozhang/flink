/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocatorUtil.checkMinimalRequiredSlots;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAssigner.createExecutionSlotSharingGroups;

/**
 * Simple {@link SlotAssigner} that selects all available slots in the minimal task executors to
 * match requests.
 */
public class DefaultSlotAssigner implements SlotAssigner {

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {
        checkMinimalRequiredSlots(jobInformation, freeSlots);

        final List<ExecutionSlotSharingGroup> allExecutionSlotSharingGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allExecutionSlotSharingGroups.addAll(
                    createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }

        Collection<? extends SlotInfo> pickedSlots = freeSlots;
        // To avoid the sort-work loading.
        if (freeSlots.size() > allExecutionSlotSharingGroups.size()) {
            final Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor =
                    getSlotsPerTaskExecutor(freeSlots);
            pickedSlots =
                    pickSlotsInMinimalTaskExecutors(
                            slotsPerTaskExecutor, allExecutionSlotSharingGroups.size());
        }

        Iterator<? extends SlotInfo> iterator = pickedSlots.iterator();
        Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : allExecutionSlotSharingGroups) {
            assignments.add(new SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }

    /**
     * In order to minimize the using of task executors at the resource manager side in the
     * session-mode and release more task executors in a timely manner, it is a good choice to
     * prioritize selecting slots on task executors with the least available slots. This strategy
     * also ensures that relatively fewer task executors can be used in application-mode.
     */
    private List<TaskManagerLocation> getSortedTaskExecutors(
            Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor) {
        final Comparator<TaskManagerLocation> taskExecutorComparator =
                Comparator.comparingInt(tml -> slotsPerTaskExecutor.get(tml).size());
        return slotsPerTaskExecutor.keySet().stream().sorted(taskExecutorComparator).toList();
    }

    /**
     * Pick the target slots to assign with the requested groups.
     *
     * @param slotsByTaskExecutor slots per task executor.
     * @param requestedGroups the number of the request execution slot sharing groups.
     * @return the target slots that are distributed on the minimal task executors.
     */
    private Collection<? extends SlotInfo> pickSlotsInMinimalTaskExecutors(
            Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor,
            int requestedGroups) {
        final List<SlotInfo> pickedSlots = new ArrayList<>();
        TaskExecutorSelector selector = new TaskExecutorSelector(
                slotsByTaskExecutor,
                requestedGroups);
        List<TaskManagerLocation> minimalTaskExecutors = selector.pickMinimalTaskExecutors();
        for (TaskManagerLocation taskExecutor : minimalTaskExecutors) {
            pickedSlots.addAll(slotsByTaskExecutor.get(taskExecutor));
        }
        return pickedSlots;
    }

    static Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> getSlotsPerTaskExecutor(
            Collection<? extends SlotInfo> slots) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                SlotInfo::getTaskManagerLocation,
                                Collectors.mapping(identity(), Collectors.toSet())));
    }

    /**
     * The class is responsible for selecting the optimal set of task executors
     * based on the number of requested groups and the available slots information for each task executor.
     * It uses a backtracking algorithm to pick the minimal set of task executors that can fulfill the requested number of slots.
     * See <a href="https://en.wikipedia.org/wiki/Backtracking">https://en.wikipedia.org/wiki/Backtracking</a> for more details
     */
    @Internal
    private class TaskExecutorSelector {
        private final Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor;
        private final int requestedGroups;
        private final List<TaskManagerLocation> allTaskExecutors;
        private final List<TaskManagerLocation> pickedTaskExecutors;
        private final List<TaskManagerLocation> minimalTaskExecutors;

        private TaskExecutorSelector(
                Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor,
                int requestedGroups) {
            this.slotsByTaskExecutor = slotsByTaskExecutor;
            this.requestedGroups = requestedGroups;
            this.allTaskExecutors = getSortedTaskExecutors(slotsByTaskExecutor);
            this.minimalTaskExecutors = new ArrayList<>(this.allTaskExecutors);
            this.pickedTaskExecutors = new ArrayList<>();
        }

        // Pick the minimal number of task executors that can satisfy the requested groups
        public List<TaskManagerLocation> pickMinimalTaskExecutors() {
            pickMinimalTaskExecutors(0);
            return minimalTaskExecutors;
        }

        // Sum the total number of slots in taskExecutors
        private int getTotalSlotsNumber(List<TaskManagerLocation> taskExecutors) {
            return taskExecutors.stream()
                    .mapToInt(tml -> slotsByTaskExecutor.get(tml).size())
                    .sum();
        }

        // Recursively search the minimal number of task executors that can satisfy the requested groups
        private void pickMinimalTaskExecutors(int index) {
            int pickedSlotsNumber = getTotalSlotsNumber(pickedTaskExecutors);
            if (pickedSlotsNumber >= requestedGroups) {
                if (pickedSlotsNumber < getTotalSlotsNumber(minimalTaskExecutors)) {
                    minimalTaskExecutors.clear();
                    minimalTaskExecutors.addAll(pickedTaskExecutors);
                }
                return;
            }

            if (index >= this.allTaskExecutors.size()) {
                return;
            }
            // Picked the current task executor
            pickedTaskExecutors.add(allTaskExecutors.get(index));
            pickMinimalTaskExecutors(index + 1);

            // After picked current task executor, update the number of picked slots
            pickedSlotsNumber = getTotalSlotsNumber(pickedTaskExecutors);

            // Unpicked the current task executor
            pickedTaskExecutors.remove(pickedTaskExecutors.size() - 1);
            // Pruning: if the current number of picked slots is not enough, continue without the current task executor
            if (pickedSlotsNumber < requestedGroups) {
                pickMinimalTaskExecutors(index + 1);
            }
        }
    }
}
