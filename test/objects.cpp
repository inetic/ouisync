#define BOOST_TEST_MODULE objects
#include <boost/test/included/unit_test.hpp>

#include "object/block.h"
#include "object/tree.h"
#include "object/io.h"
#include "namespaces.h"
#include "hex.h"
#include "array_io.h"

#include <iostream>
#include <random>
#include <boost/filesystem.hpp>
#include <boost/variant.hpp>
#include <boost/range/distance.hpp>
#include <boost/serialization/string.hpp>
#include <boost/range/adaptor/filtered.hpp>

using namespace std;
using namespace ouisync;

using cpputils::Data;
using object::Tree;
using object::Block;
using object::Id;
using boost::variant;

struct Random {
    Random() : gen(std::random_device()()) {}

    Data data(size_t size) {
        Data d(size);
        auto ptr = static_cast<char*>(d.data());
        fill(ptr, size);
        return d;
    }

    std::vector<char> vector(size_t size) {
        std::vector<char> v(size);
        auto ptr = static_cast<char*>(v.data());
        fill(ptr, size);
        return v;
    }

    std::string string(size_t size) {
        std::string result(size, '\0');
        fill(&result[0], size);
        return result;
    }

    Id object_id() {
        Id id;
        fill(reinterpret_cast<char*>(id.data()), id.size());
        return id;
    }

    void fill(char* ptr, size_t size) {
        std::uniform_int_distribution<> distrib(0, 255);
        for (size_t i = 0; i < size; ++i) ptr[i] = distrib(gen);
    }

    std::mt19937 gen;
};

#define REQUIRE_HEX_EQUAL(a, b) \
    BOOST_REQUIRE_EQUAL(to_hex<char>(a), to_hex<char>(b));

bool is_refcount(const fs::path& path) {
    return path.extension() == ".rc";
}

auto files_in(const fs::path& path) {
    return fs::recursive_directory_iterator(path) |
        boost::adaptors::filtered([](auto p) {
                return fs::is_regular_file(p); });
}

auto objects_in(const fs::path& path) {
    return files_in(path) |
        boost::adaptors::filtered([](auto p) { return !is_refcount(p.path()); });
}

size_t count_files(const fs::path& path) {
    return boost::distance(files_in(path));
}

size_t count_objects(const fs::path& path) {
    return boost::distance(objects_in(path));
}

fs::path choose_test_dir() {
    return fs::unique_path("/tmp/ouisync/test-objects-%%%%-%%%%-%%%%-%%%%");
}

BOOST_AUTO_TEST_CASE(block_is_same) {
    fs::path testdir = choose_test_dir();

    Random random;
    Data data(random.data(1000));
    object::Block b1(data);
    b1.store(testdir);
    auto b2 = object::io::load<object::Block>(testdir, b1.calculate_id());
    REQUIRE_HEX_EQUAL(b1.calculate_id(), b2.calculate_id());
}

BOOST_AUTO_TEST_CASE(tree_is_same) {
    fs::path testdir = choose_test_dir();

    Random random;
    object::Tree t1;
    t1[random.string(2)]  = random.object_id();
    t1[random.string(10)] = random.object_id();
    t1.store(testdir);
    auto t2 = object::io::load<object::Tree>(testdir, t1.calculate_id());
    REQUIRE_HEX_EQUAL(t1.calculate_id(), t2.calculate_id());
}

BOOST_AUTO_TEST_CASE(tree_path) {
    fs::path objdir = choose_test_dir();

    Random random;

    Data data(random.data(1000));
    Block b1(data);

    Tree root;

    auto old_root_id = root.store(objdir);
    BOOST_REQUIRE(fs::exists(objdir/object::path::from_id(old_root_id)));

    auto new_root_id = object::io::store(objdir, old_root_id, "foo/bar", b1);

    BOOST_REQUIRE(!fs::exists(objdir/object::path::from_id(old_root_id)));
    BOOST_REQUIRE_EQUAL(count_objects(objdir), 3);

    auto b2 = object::io::load(objdir, new_root_id, "foo/bar");

    REQUIRE_HEX_EQUAL(b1.calculate_id(), b2.calculate_id());
}

//--------------------------------------------------------------------
Id store(const fs::path& objdir, const vector<char>& v) {
    Data d(v.size());
    memcpy(d.data(), v.data(), v.size());
    Block block(d);
    return block.store(objdir);
}

Id store(const fs::path& objdir, const Id& root, const fs::path path, const Data& d) {
    Block block(d);
    return object::io::store(objdir, root, path, d);
}

//--------------------------------------------------------------------

BOOST_AUTO_TEST_CASE(tree_remove) {
    fs::path objdir = choose_test_dir();

    Random random;

    // Delete data from root
    {
        auto data_id = store(objdir, random.vector(256));

        Tree root;
        root.insert({"data", data_id});

        auto root_id = object::io::store(objdir, root);

        BOOST_REQUIRE_EQUAL(count_objects(objdir), 2);

        auto opt_root_id = object::io::remove(objdir, root_id, "data");
        BOOST_REQUIRE(opt_root_id);
        root_id = *opt_root_id;

        root = object::io::load<Tree>(objdir, root_id);

        BOOST_REQUIRE_EQUAL(count_objects(objdir), 1);
        BOOST_REQUIRE_EQUAL(root.size(), 0);

        BOOST_REQUIRE(object::io::remove(objdir, root_id));
        BOOST_REQUIRE_EQUAL(count_objects(objdir), 0);
    }

    // Delete data from subdir, then delete the subdir
    {
        auto data_id = store(objdir, random.vector(256));

        Tree dir;
        dir.insert({"data", data_id});
        auto dir_id = object::io::store(objdir, dir);

        Tree root;
        root.insert({"dir", dir_id});
        auto root_id = object::io::store(objdir, root);

        BOOST_REQUIRE_EQUAL(count_objects(objdir), 3);

        auto opt_root_id = object::io::remove(objdir, root_id, "dir/data");
        BOOST_REQUIRE(opt_root_id);
        root_id = *opt_root_id;
        root = object::io::load<Tree>(objdir, root_id);

        BOOST_REQUIRE_EQUAL(root.size(), 1);
        BOOST_REQUIRE_EQUAL(count_objects(objdir), 2);

        opt_root_id = object::io::remove(objdir, root_id, "dir");
        BOOST_REQUIRE(opt_root_id);
        root_id = *opt_root_id;
        root = object::io::load<Tree>(objdir, root_id);

        BOOST_REQUIRE_EQUAL(root.size(), 0);
        BOOST_REQUIRE_EQUAL(count_objects(objdir), 1);

        BOOST_REQUIRE(object::io::remove(objdir, root_id));

        BOOST_REQUIRE_EQUAL(count_objects(objdir), 0);
    }

    // Delete subdir, check data is deleted with it
    {
        auto data_id = store(objdir, random.vector(256));

        Tree dir;
        dir.insert({"data", data_id});
        auto dir_id = object::io::store(objdir, dir);

        Tree root;
        root.insert({"dir", dir_id});
        auto root_id = object::io::store(objdir, root);

        BOOST_REQUIRE_EQUAL(count_objects(objdir), 3);

        auto opt_root_id = object::io::remove(objdir, root_id, "dir");
        BOOST_REQUIRE(opt_root_id);
        root_id = *opt_root_id;
        root = object::io::load<Tree>(objdir, root_id);

        BOOST_REQUIRE_EQUAL(root.size(), 0);
        BOOST_REQUIRE_EQUAL(count_objects(objdir), 1);

        object::io::remove(objdir, root_id);
        BOOST_REQUIRE_EQUAL(count_objects(objdir), 0);
    }

    // Delete data from one root, preserve (using refcount) in the other
    {
        Data data  = random.data(256);

        // These are to make sure the two roots are different objects.
        Data data1 = random.data(256);
        Data data2 = random.data(256);

        Id root1_id, root2_id;

        root1_id = object::io::store(objdir, Tree{});
        root1_id = store(objdir, root1_id, "data", data);
        root1_id = store(objdir, root1_id, "data1", data1);

        root2_id = object::io::store(objdir, Tree{});
        root2_id = store(objdir, root2_id, "data", data);
        root2_id = store(objdir, root2_id, "data2", data2);

        BOOST_REQUIRE_EQUAL(count_objects(objdir), 2 /* roots */ + 3 /* data */);

        auto opt_root1_id = object::io::remove(objdir, root1_id, "data");
        BOOST_REQUIRE(opt_root1_id);
        root1_id = *opt_root1_id;
        Tree root1 = object::io::load<Tree>(objdir, root1_id);

        BOOST_REQUIRE_EQUAL(root1.size(), 1);
        // Since root2 still has "data", we expect it to be not deleted.
        BOOST_REQUIRE_EQUAL(count_objects(objdir), 2 /* roots */ + 3 /* data */);

        auto opt_root2_id = object::io::remove(objdir, root2_id, "data");
        BOOST_REQUIRE(opt_root2_id);
        root2_id = *opt_root2_id;
        Tree root2 = object::io::load<Tree>(objdir, root2_id);

        BOOST_REQUIRE_EQUAL(root2.size(), 1);
        // Since root2 still has "data", we expect it to be not deleted.
        BOOST_REQUIRE_EQUAL(count_objects(objdir), 2 /* roots */ + 2 /* data */);
    }
}