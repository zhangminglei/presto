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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.planner.plan.PlanNodeId;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LimitOperator
        implements Operator
{
    public static class LimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final long offset;
        private final long limit;
        private boolean closed;
        private final boolean partial;

        public LimitOperatorFactory(int operatorId, PlanNodeId planNodeId, long offset, long limit, boolean partial)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.offset = offset;
            this.limit = limit;
            this.partial = partial;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LimitOperator.class.getSimpleName());

            if (partial) {
                return new LimitOperator(operatorContext, 0, offset + limit);
            }
            else {
                return new LimitOperator(operatorContext, offset, limit);
            }
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new LimitOperatorFactory(operatorId, planNodeId, offset, limit, partial);
        }
    }

    private final OperatorContext operatorContext;
    private Page nextPage;
    private long offset;
    private long remainingLimit;

    public LimitOperator(OperatorContext operatorContext, long offset, long limit)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        checkArgument(offset >= 0, "offset must be at least zero");
        this.offset = offset;
        checkArgument(limit >= 0, "limit must be at least zero");
        this.remainingLimit = limit;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        remainingLimit = 0;
    }

    @Override
    public boolean isFinished()
    {
        return remainingLimit == 0 && nextPage == null;
    }

    @Override
    public boolean needsInput()
    {
        return remainingLimit > 0 && nextPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());

        if (offset >= page.getPositionCount()) {
            offset -= page.getPositionCount();
        }
        else {
            if (offset == 0 && remainingLimit >= page.getPositionCount()) {
                nextPage = page;
                remainingLimit -= page.getPositionCount();
            }
            else {
                long startPos = (int) (page.getPositionCount() - offset);
                long cntInPage = remainingLimit >= startPos ? startPos : remainingLimit;

                Block[] blocks = new Block[page.getChannelCount()];
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    Block block = page.getBlock(channel);
                    blocks[channel] = block.getRegion((int) offset, (int) cntInPage);
                }
                nextPage = new Page((int) cntInPage, blocks);
                remainingLimit -= cntInPage;
                offset = 0;
            }
        }
    }

    @Override
    public Page getOutput()
    {
        Page page = nextPage;
        nextPage = null;
        return page;
    }
}
