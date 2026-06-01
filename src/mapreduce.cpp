#include "mapreduce.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <regex>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <chrono>
#include <ctime>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>
#include <filesystem>

namespace fs = std::filesystem;

// Internal helpers

static std::mutex g_log_mtx;

void log(const std::string &msg)
{
    auto now = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) %1000;
    char tbuf[32];
    std::strftime(tbuf, sizeof(tbuf), "%H:%M:%S", std::localtime(&tt));

    std::lock_guard<std::mutex> lk(g_log_mtx);
    std::cout << "[" << tbuf << "."<< std::setw(3) << std::setfill('0') << ms.count() << "] "<< msg << "\n"<< std::flush;
}

// path helpers

std::string inter_file_path(const std::string &inter_dir,int map_id, int reduce_id)
{
    return inter_dir + "/mr-" + std::to_string(map_id) + "-" + std::to_string(reduce_id) + ".txt";
}

// FNV-1a hash for partition selection

int partition_key(const std::string &key, int r_count)
{
    if (r_count <= 0)
        return 0;
    // FNV-1a 32-bit
    uint32_t hash = 2166136261u;
    for (unsigned char c : key)
    {
        hash ^= c;
        hash *= 16777619u;
    }
    return static_cast<int>(hash % static_cast<uint32_t>(r_count));
}

// socket I/O

bool send_all(int fd, const std::string &data)
{
    size_t total = 0;
    const char *ptr = data.c_str();
    while (total < data.size())
    {
        ssize_t sent = ::send(fd, ptr + total, data.size() - total, MSG_NOSIGNAL);
        if (sent <= 0)
        {
            if (errno == EINTR)
                continue;
            return false;
        }
        total += static_cast<size_t>(sent);
    }
    return true;
}

std::string recv_until_end(int fd)
{
    std::string buf;
    buf.reserve(1024);
    char ch;

    while (true)
    {
        ssize_t n = ::recv(fd, &ch, 1, 0);
        if (n <= 0)
            return {};
        buf += ch;
        if (buf.size() >= 5 &&
            buf.compare(buf.size() - 5, 5, "\nEND\n") == 0)
        {
            return buf;
        }
        if (buf.size() > MAX_MSG_SIZE)
            return {};
    }
}

// Map function

void map_fn(const std::string &input_file,const std::string &pattern,int r_count,const std::string &inter_dir,int task_id)
{
    log("MAP  task=" + std::to_string(task_id) + "  file=" + input_file + "  pattern='" + pattern + "'");

    std::vector<std::ofstream> inter_files(r_count);
    for (int r = 0; r < r_count; ++r)
    {
        std::string path = inter_file_path(inter_dir, task_id, r);
        inter_files[r].open(path, std::ios::app);
        if (!inter_files[r])
        {
            throw std::runtime_error("Cannot open intermediate file: " + path);
        }
    }

    std::regex re;
    try
    {
        re = std::regex(pattern, std::regex::ECMAScript | std::regex::optimize);
    }
    catch (const std::regex_error &e)
    {
        throw std::runtime_error(std::string("Bad regex pattern: ") + e.what());
    }

    std::ifstream in(input_file);
    if (!in)
        throw std::runtime_error("Cannot open input file: " + input_file);

    std::string line;
    size_t matched = 0, total = 0;
    while (std::getline(in, line))
    {
        ++total;
        if (std::regex_search(line, re))
        {
            ++matched;
            int part = partition_key(line, r_count);
            inter_files[part] << line << "\t\n";
        }
    }

    log("MAP  task=" + std::to_string(task_id) + "  scanned=" + std::to_string(total) + "  matched=" + std::to_string(matched));
}

// Reduce function

void reduce_fn(int partition,
               const std::string &inter_dir,
               const std::string &output_file)
{
    log("REDUCE  partition=" + std::to_string(partition) + "  output=" + output_file);

    std::ofstream out(output_file, std::ios::app);
    if (!out)
        throw std::runtime_error("Cannot open output file: " + output_file);

    std::vector<fs::path> inter_paths;
    for (const auto &entry : fs::directory_iterator(inter_dir))
    {
        const std::string fname = entry.path().filename().string();
        std::string suffix = "-" + std::to_string(partition) + ".txt";
        if (fname.rfind("mr-", 0) == 0 &&
            fname.size() > suffix.size() &&
            fname.compare(fname.size() - suffix.size(),
                          suffix.size(), suffix) == 0)
        {
            inter_paths.push_back(entry.path());
        }
    }

    std::sort(inter_paths.begin(), inter_paths.end());

    size_t line_count = 0;
    for (const auto &p : inter_paths)
    {
        std::ifstream in(p);
        if (!in)
            continue;
        std::string line;
        while (std::getline(in, line))
        {
            if (line.empty())
                continue;

            if (!line.empty() && line.back() == '\t')
            {
                line.pop_back();
            }
            out << line << "\n";
            ++line_count;
        }
    }

    log("REDUCE  partition=" + std::to_string(partition) + "  lines_written=" + std::to_string(line_count));
}
