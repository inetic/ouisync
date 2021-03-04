#pragma once

#include "shortcuts.h"

#include <boost/filesystem/path.hpp>
#include <boost/program_options.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/optional.hpp>

namespace ouisync {

class Options {
public:
    Options();

    void parse(unsigned args, char** argv);

    void write_help(std::ostream&);

    bool help;

    fs::path basedir;
    fs::path blockdir;
    fs::path user_id_file_path;

    Opt<fs::path> mountdir;

    Opt<net::ip::tcp::endpoint> accept_endpoint;
    Opt<net::ip::tcp::endpoint> connect_endpoint;

    struct Branch {
        fs::path blockdir;
    };

    operator Branch() const {
        return { blockdir };
    }

private:
    boost::program_options::options_description description;
};

} // namespace
