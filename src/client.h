#pragma once

#include "message_broker.h"
#include "branch.h"

namespace ouisync {

class RemoteBranch;

class Client {
private:
    using ServerStateCounter = decltype(RsNotifyOnChange::new_state);

public:
    Client(MessageBroker::Client&&, Branch&);

    net::awaitable<void> run(Cancel);

private:
    template<class T> net::awaitable<T> receive(Cancel);

    net::awaitable<Index> fetch_index(Cancel);
    net::awaitable<Opt<Block>> fetch_block(const BlockId&, Cancel);
    net::awaitable<void> wait_for_a_change(Cancel);

private:
    MessageBroker::Client _broker;
    Branch& _branch;
    Opt<ServerStateCounter> _last_server_state;
};

} // namespace
