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
package com.facebook.presto.operator;

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 * <p>
 * This operator supports limit offset,limit functionality. For example: a sql query `select col1 from table order by col2 desc limit offset,limit`
 * offset means the dropped rows from the returned table, whereas limit means the needing rows in that table.
 */
public class TopNOperator
        implements Operator
{
    public static class TopNOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final int offset;
        private final int limit;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int offset,
                int limit,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.offset = offset;
            this.limit = limit;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNOperator.class.getSimpleName());
            return new TopNOperator(
                    operatorContext,
                    sourceTypes,
                    offset,
                    limit,
                    sortChannels,
                    sortOrders);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNOperatorFactory(operatorId, planNodeId, sourceTypes, offset, limit, sortChannels, sortOrders);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;

    private GroupedTopNBuilder topNBuilder;
    private boolean finishing;

    private Iterator<Page> outputIterator;
    private int offset;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int offset,
            int limit,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        checkArgument(offset >= 0, "offset must be positive");
        this.offset = offset;
        checkArgument(limit >= 0, "limit must be positive");

        if (limit == 0) {
            finishing = true;
            outputIterator = emptyIterator();
        }
        else {
            topNBuilder = new GroupedTopNBuilder(
                    types,
                    new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                    offset,
                    limit,
                    false,
                    new NoChannelGroupByHash());
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && noMoreOutput();
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && !noMoreOutput();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        boolean done = topNBuilder.processPage(requireNonNull(page, "page is null")).process();
        // there is no grouping so work will always be done
        verify(done);
        updateMemoryReservation();
    }

    @Override
    public Page getOutput()
    {
        if (!finishing || noMoreOutput()) {
            return null;
        }

        if (outputIterator == null) {
            // start flushing
            outputIterator = topNBuilder.buildResult();
        }

        Page output = null;
        if (outputIterator.hasNext()) {
            output = outputIterator.next();
        }
        else {
            outputIterator = emptyIterator();
        }
        updateMemoryReservation();

        int positionCount = output.getPositionCount();
        if (offset >= positionCount) {
            offset -= positionCount;
            output = null;
        }
        else {
            if (offset == 0) {
                return output;
            }
            else {
                int remainingInPage = output.getPositionCount() - offset;
                Block[] blocks = new Block[output.getChannelCount()];
                for (int channel = 0; channel < output.getChannelCount(); channel++) {
                    Block block = output.getBlock(channel);
                    blocks[channel] = block.getRegion(offset, remainingInPage);
                }
                output = new Page(remainingInPage, blocks);
                offset = 0;
            }
        }
        return output;
    }

    private void updateMemoryReservation()
    {
        localUserMemoryContext.setBytes(topNBuilder.getEstimatedSizeInBytes());
    }

    private boolean noMoreOutput()
    {
        return outputIterator != null && !outputIterator.hasNext();
    }
}
