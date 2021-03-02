#pragma once

#include "object_tag.h"
#include "object_id.h"
#include "shortcuts.h"
#include "block_store.h"

//#include <boost/serialization/array_wrapper.hpp>
//#include <boost/serialization/split_member.hpp>

namespace ouisync {

struct FileBlob : std::vector<uint8_t>
{
public:
    static constexpr ObjectTag tag = ObjectTag::FileBlob;

    struct Nothing {
        static constexpr ObjectTag tag = FileBlob::tag;

        template<class Archive>
        void load(Archive& ar, const unsigned int version) {}

        BOOST_SERIALIZATION_SPLIT_MEMBER()
    };

    struct Size {
        static constexpr ObjectTag tag = FileBlob::tag;

        uint32_t value;

        template<class Archive>
        void load(Archive& ar, const unsigned int version) {
            ar & value;
        }

        BOOST_SERIALIZATION_SPLIT_MEMBER()
    };

    using Parent = std::vector<uint8_t>;
    using std::vector<uint8_t>::vector;

    FileBlob(const Parent& p) : Parent(p) {}
    FileBlob(Parent&& p) : Parent(std::move(p)) {}

    ObjectId calculate_id() const;

    ObjectId save(BlockStore&) const;
    bool maybe_load(const BlockStore::Block&);

    void load(const BlockStore::Block& block)
    {
        if (!maybe_load(block)) {
            throw std::runtime_error("FileBlob:: Failed to load from block");
        }
    }

    static
    size_t read_size(const BlockStore::Block&);

    friend std::ostream& operator<<(std::ostream&, const FileBlob&);

private:
};

} // namespace

