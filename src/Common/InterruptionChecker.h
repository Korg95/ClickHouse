#pragma once

#include <Common/ErrorCodes.h>
#include <Common/Exception.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ProcessList.h>

namespace DB
{

/** Helper that periodically checks whether the current query has been cancelled
  * or timed out. It should be used in loops that may block on external resources
  * to make sure that max_execution_time and KILL QUERY signals are respected.
  */
class InterruptionChecker
{
public:
    InterruptionChecker() = default;

    InterruptionChecker(ContextPtr context_, String cancel_message_, String timeout_message_)
        : context(context_)
        , cancel_message(std::move(cancel_message_))
        , timeout_message(std::move(timeout_message_))
    {
    }

    void setMessages(String cancel_message_, String timeout_message_)
    {
        cancel_message = std::move(cancel_message_);
        timeout_message = std::move(timeout_message_);
    }

    void reset(ContextPtr context_, String cancel_message_, String timeout_message_)
    {
        context = context_;
        cancel_message = std::move(cancel_message_);
        timeout_message = std::move(timeout_message_);
    }

    void check() const
    {
        auto context_ptr = context.lock();
        if (!context_ptr)
            return;

        if (context_ptr->isCurrentQueryKilled())
            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, cancel_message);

        if (auto process_list_element = context_ptr->getProcessListElementSafe())
        {
            if (!process_list_element->checkTimeLimitSoft())
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, timeout_message);
        }
    }

private:
    ContextWeakPtr context;
    String cancel_message;
    String timeout_message;
};

}

