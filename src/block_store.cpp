#include <block_store.h>
#include <block_sync.h>
#include <blockstore/implementations/ondisk/OnDiskBlockStore2.h>

using namespace ouisync;
using namespace std;

using BlockId = blockstore::BlockId;

inline auto digest(const cpputils::Data& data) {
    Sha256 hash;
    hash.update(data.data(), data.size());
    return hash.close();
}

BlockStore::BlockStore(fs::path basedir, unique_ptr<BlockSync> sync)
    : _bs(std::make_unique<blockstore::ondisk::OnDiskBlockStore2>(std::move(basedir)))
    , _sync(move(sync))
{}

BlockId BlockStore::createBlockId() const {
    auto r = _bs->createBlockId();
    return r;
}

bool BlockStore::tryCreate(const BlockId &blockId, const cpputils::Data &data) {
    bool created = _bs->tryCreate(blockId, data);
    if (created) {
        _sync->add_action(BlockSync::ActionCreateBlock{blockId, digest(data)});
    }
    return created;
}

bool BlockStore::remove(const BlockId &blockId) {
    auto removed = _bs->remove(blockId);
    if (removed) {
        _sync->add_action(BlockSync::ActionRemoveBlock{blockId});
    }
    return removed;
}

boost::optional<cpputils::Data> BlockStore::load(const BlockId &blockId) const {
    return _bs->load(blockId);
}

// Store the block with the given blockId. If it doesn't exist, it is created.
void BlockStore::store(const BlockId &blockId, const cpputils::Data &data) {
    _bs->store(blockId, data);
    _sync->add_action(BlockSync::ActionModifyBlock{blockId, digest(data)});
}

uint64_t BlockStore::numBlocks() const {
    auto r = _bs->numBlocks();
    return r;
}

uint64_t BlockStore::estimateNumFreeBytes() const {
    auto r = _bs->estimateNumFreeBytes();
    return r;
}

uint64_t BlockStore::blockSizeFromPhysicalBlockSize(uint64_t blockSize) const {
    auto r = _bs->blockSizeFromPhysicalBlockSize(blockSize);
    return r;
}

void BlockStore::forEachBlock(std::function<void (const BlockId &)> callback) const {
    _bs->forEachBlock(std::move(callback));
}

BlockStore::~BlockStore() {}
