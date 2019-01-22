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

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class LimitNode
        extends PlanNode
{
    private final PlanNode source;
    private final long offset;
    private final long limit;
    private final boolean partial;

    public LimitNode(
            PlanNodeId id,
            PlanNode source,
            long count,
            boolean partial)
    {
        this(id, source, 0, count, partial);
    }

    @JsonCreator
    public LimitNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("offset") long offset,
            @JsonProperty("limit") long limit,
            @JsonProperty("partial") boolean partial)
    {
        super(id);
        this.partial = partial;

        requireNonNull(source, "source is null");
        checkArgument(offset >= 0, "offset must be greater than or equal to zero");
        checkArgument(limit >= 0, "count must be greater than or equal to zero");

        this.source = source;
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    @JsonProperty
    public long getOffset()
    {
        return offset;
    }

    @JsonProperty
    public boolean isPartial()
    {
        return partial;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitLimit(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new LimitNode(getId(), Iterables.getOnlyElement(newChildren), offset, limit, isPartial());
    }
}
