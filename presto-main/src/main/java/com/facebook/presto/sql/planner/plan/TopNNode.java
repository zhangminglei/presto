/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class TopNNode
        extends PlanNode
{
    public enum Step
    {
        SINGLE,
        PARTIAL,
        FINAL
    }

    private final PlanNode source;
    private final long offset;
    private final long limit;
    private final OrderingScheme orderingScheme;
    private final Step step;

    @JsonCreator
    public TopNNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("offset") long offset,
            @JsonProperty("limit") long limit,
            @JsonProperty("orderingScheme") OrderingScheme orderingScheme,
            @JsonProperty("step") Step step)
    {
        super(id);

        requireNonNull(source, "source is null");
        checkArgument(offset >= 0, "offset must be positive");
        checkArgument(limit >= 0, "count must be positive");
        checkCondition(limit <= Integer.MAX_VALUE, NOT_SUPPORTED, "ORDER BY LIMIT > %s is not supported", Integer.MAX_VALUE);
        requireNonNull(orderingScheme, "orderingScheme is null");

        this.source = source;
        this.offset = offset;
        this.limit = limit;
        this.orderingScheme = orderingScheme;
        this.step = requireNonNull(step, "step is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @JsonProperty("offset")
    public long getOffset()
    {
        return offset;
    }

    @JsonProperty("limit")
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty("orderingScheme")
    public OrderingScheme getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty("step")
    public Step getStep()
    {
        return step;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitTopN(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new TopNNode(getId(), Iterables.getOnlyElement(newChildren), offset, limit, orderingScheme, step);
    }
}
