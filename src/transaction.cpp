#include "transaction.h"
#include "block_store.h"
#include "index.h"

using namespace ouisync;

void Transaction::commit(const UserId& user, BlockStore& block_store, Index& index) {
    for (auto& [block_id, block] : _blocks) {
        block_store.store(block_id, block);
    }

    for (auto& [from, to] : _edges) {
        index.insert_object(user, to, from);
    }
}