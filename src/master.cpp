#include "mapreduce.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <algorithm>
#include <iomanip>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>

namespace fs = std::filesystem;

// ────────────────────────────────────────────────────────────────────────────
// Master state (protected by g_mtx)
// ────────────────────────────────────────────────────────────────────────────

static std::mutex              g_mtx;
static std::vector<MapTask>    g_map_tasks;
static std::vector<ReduceTask> g_reduce_tasks;
static std::atomic<bool>       g_all_done{false};

// Timeout (seconds) before a task is considered failed and re-queued.
static constexpr int TASK_TIMEOUT_SEC = 30;

// Task scheduling helpers

static bool all_maps_done()
{
    for (const auto& t : g_map_tasks)
        if (t.status != TaskStatus::COMPLETED) return false;
    return true;
}


static bool all_reduces_done()
{
    for (const auto& t : g_reduce_tasks)
        if (t.status != TaskStatus::COMPLETED) return false;
    return true;
}


static MapTask* pick_idle_map(int worker_fd)
{
    time_t now = time(nullptr);
    for (auto& t : g_map_tasks) {
        if (t.status == TaskStatus::IDLE) {
            t.status      = TaskStatus::IN_PROGRESS;
            t.worker_fd   = worker_fd;
            t.assigned_at = now;
            return &t;
        }
        if (t.status == TaskStatus::IN_PROGRESS &&
            (now - t.assigned_at) > TASK_TIMEOUT_SEC) {
            log("MASTER  Re-queuing timed-out MAP task=" + std::to_string(t.id));
            t.status      = TaskStatus::IN_PROGRESS;
            t.worker_fd   = worker_fd;
            t.assigned_at = now;
            return &t;
        }
    }
    return nullptr;
}


static ReduceTask* pick_idle_reduce(int worker_fd)
{
    time_t now = time(nullptr);
    for (auto& t : g_reduce_tasks) {
        if (t.status == TaskStatus::IDLE) {
            t.status      = TaskStatus::IN_PROGRESS;
            t.worker_fd   = worker_fd;
            t.assigned_at = now;
            return &t;
        }
        if (t.status == TaskStatus::IN_PROGRESS &&
            (now - t.assigned_at) > TASK_TIMEOUT_SEC) {
            log("MASTER  Re-queuing timed-out REDUCE task=" + std::to_string(t.id));
            t.status      = TaskStatus::IN_PROGRESS;
            t.worker_fd   = worker_fd;
            t.assigned_at = now;
            return &t;
        }
    }
    return nullptr;
}

// Protocol helpers


static bool send_map_task(int fd, const MapTask& t)
{
    std::ostringstream oss;
    oss << "TASK_TYPE MAP\n"
        << "TASK_ID "   << t.id           << "\n"
        << "INPUT "     << t.input_file   << "\n"
        << "PATTERN "   << t.pattern      << "\n"
        << "R_COUNT "   << t.r_count      << "\n"
        << "INTER_DIR " << t.inter_dir    << "\n"
        << "END\n";
    return send_all(fd, oss.str());
}


static bool send_reduce_task(int fd, const ReduceTask& t)
{
    std::ostringstream oss;
    oss << "TASK_TYPE REDUCE\n"
        << "TASK_ID "   << t.id          << "\n"
        << "PARTITION " << t.partition   << "\n"
        << "INTER_DIR " << t.inter_dir   << "\n"
        << "OUTPUT "    << t.output_file << "\n"
        << "END\n";
    return send_all(fd, oss.str());
}


static bool send_done(int fd)
{
    return send_all(fd, "TASK_TYPE DONE\nEND\n");
}

// Worker handler 

static void handle_worker(int worker_fd, const std::string& worker_addr)
{
    log("MASTER  Worker connected from " + worker_addr);

    // Each interaction: worker sends a request, master responds with a task.
    // Loop until the job is done or the socket closes.
    while (!g_all_done.load()) {
        // 1. Receive request from worker.
        std::string req = recv_until_end(worker_fd);
        if (req.empty()) {
            log("MASTER  Worker " + worker_addr + " disconnected");
            break;
        }

        // Parse request type.
        if (req.rfind("WORKER_READY", 0) == 0 ||
            req.rfind("TASK_DONE", 0) == 0) {

            // Handle completion notifications first.
            if (req.rfind("TASK_DONE", 0) == 0) {
                // Format: "TASK_DONE <id> <MAP|REDUCE>\nEND\n"
                std::istringstream iss(req);
                std::string token, stype;
                int id = -1;
                iss >> token >> id >> stype;

                std::lock_guard<std::mutex> lk(g_mtx);
                if (stype == "MAP") {
                    for (auto& t : g_map_tasks)
                        if (t.id == id && t.status == TaskStatus::IN_PROGRESS) {
                            t.status = TaskStatus::COMPLETED;
                            log("MASTER  MAP task=" + std::to_string(id) + " COMPLETED");
                            break;
                        }
                } else if (stype == "REDUCE") {
                    for (auto& t : g_reduce_tasks)
                        if (t.id == id && t.status == TaskStatus::IN_PROGRESS) {
                            t.status = TaskStatus::COMPLETED;
                            log("MASTER  REDUCE task=" + std::to_string(id) + " COMPLETED");
                            break;
                        }
                }
            }
        }

        // 2. Assign the next available task.
        std::unique_lock<std::mutex> lk(g_mtx);

        // Check global completion.
        if (all_maps_done() && all_reduces_done()) {
            g_all_done.store(true);
            lk.unlock();
            send_done(worker_fd);
            break;
        }

        // Prefer map tasks; fall back to reduce (only after all maps complete).
        MapTask*    mt = pick_idle_map(worker_fd);
        ReduceTask* rt = nullptr;

        if (!mt && all_maps_done()) {
            rt = pick_idle_reduce(worker_fd);
        }
        lk.unlock();

        if (mt) {
            log("MASTER  Assigning MAP  task=" + std::to_string(mt->id)
                + " to " + worker_addr);
            if (!send_map_task(worker_fd, *mt)) break;

        } else if (rt) {
            log("MASTER  Assigning REDUCE task=" + std::to_string(rt->id)
                + " to " + worker_addr);
            if (!send_reduce_task(worker_fd, *rt)) break;

        } else {
            // No task available right now – tell worker to wait briefly,
            // then re-request (simulate "wait for map phase to finish").
            if (!send_all(worker_fd, "TASK_TYPE WAIT\nEND\n")) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    // Final DONE in case we exited the loop due to g_all_done.
    if (g_all_done.load()) send_done(worker_fd);

    ::close(worker_fd);
    log("MASTER  Closed connection to " + worker_addr);
}

// Task initialisation

static void init_tasks(const JobSpec& spec)
{
    // Map tasks: one per input file (each file is a "split").
    int mid = 0;
    for (const auto& f : spec.input_files) {
        MapTask t;
        t.id         = mid++;
        t.input_file = f;
        t.pattern    = spec.pattern;
        t.r_count    = spec.r_count;
        t.inter_dir  = spec.inter_dir;
        g_map_tasks.push_back(std::move(t));
    }
    log("MASTER  Initialised " + std::to_string(g_map_tasks.size())
        + " map tasks");

    // Reduce tasks: one per partition.
    for (int r = 0; r < spec.r_count; ++r) {
        ReduceTask t;
        t.id          = r;
        t.partition   = r;
        t.inter_dir   = spec.inter_dir;
        t.output_file = spec.output_dir + "/output-" + std::to_string(r) + ".txt";
        g_reduce_tasks.push_back(std::move(t));
    }
    log("MASTER  Initialised " + std::to_string(g_reduce_tasks.size())
        + " reduce tasks");
}

// Status monitor thread – prints a summary every 5 s
static void status_monitor()
{
    while (!g_all_done.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        if (g_all_done.load()) break;

        std::lock_guard<std::mutex> lk(g_mtx);
        int mc = 0, mi = 0, md = 0;
        for (const auto& t : g_map_tasks) {
            if (t.status == TaskStatus::IDLE)        ++mc;
            else if (t.status == TaskStatus::IN_PROGRESS) ++mi;
            else ++md;
        }
        int rc = 0, ri = 0, rd = 0;
        for (const auto& t : g_reduce_tasks) {
            if (t.status == TaskStatus::IDLE)        ++rc;
            else if (t.status == TaskStatus::IN_PROGRESS) ++ri;
            else ++rd;
        }                                           
        log("STATUS  Map[idle=" + std::to_string(mc)
            + " prog=" + std::to_string(mi)
            + " done=" + std::to_string(md) + "]  "
            "Reduce[idle=" + std::to_string(rc)
            + " prog=" + std::to_string(ri)
            + " done=" + std::to_string(rd) + "]");
    }
}


int main(int argc, char* argv[])
{
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0]
                  << " <pattern> <input_dir> <output_dir> <inter_dir> [r_count]\n";
        return 1;
    }

    JobSpec spec;
    spec.pattern    = argv[1];
    spec.output_dir = argv[3];
    spec.inter_dir  = argv[4];
    spec.r_count    = (argc >= 6) ? std::stoi(argv[5]) : 2;

    // Gather input files from input_dir.
    const std::string input_dir = argv[2];
    for (const auto& entry : fs::directory_iterator(input_dir)) {
        if (entry.is_regular_file()) {
            spec.input_files.push_back(entry.path().string());
        }
    }
    std::sort(spec.input_files.begin(), spec.input_files.end());

    if (spec.input_files.empty()) {
        std::cerr << "No input files found in " << input_dir << "\n";
        return 1;
    }

    // Ensure output and inter directories exist.
    fs::create_directories(spec.output_dir);
    fs::create_directories(spec.inter_dir);

    log("MASTER  Starting  pattern='" + spec.pattern + "'"
        + "  M=" + std::to_string(spec.input_files.size())
        + "  R=" + std::to_string(spec.r_count));

    init_tasks(spec);

    // Open listening socket 
    int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    ::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(MASTER_PORT);

    if (::bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (::listen(server_fd, CONN_BACKLOG) < 0) {
        perror("listen"); return 1;
    }

    log("MASTER  Listening on port " + std::to_string(MASTER_PORT)
        + "  (waiting for workers…)");

    // Start status monitor 
    std::thread monitor_thread(status_monitor);
    monitor_thread.detach();

    // Accept loop  one thread per worker 
    std::vector<std::thread> worker_threads;

    while (!g_all_done.load()) {
        sockaddr_in client_addr{};
        socklen_t   client_len = sizeof(client_addr);

        // Non-blocking accept via select with 1-second timeout.
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(server_fd, &rfds);
        timeval tv{1, 0};
        int sel = ::select(server_fd + 1, &rfds, nullptr, nullptr, &tv);
        if (sel <= 0) continue;  // timeout or signal

        int client_fd = ::accept(server_fd,
                                 reinterpret_cast<sockaddr*>(&client_addr),
                                 &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        std::string addr_str = inet_ntoa(client_addr.sin_addr);
        addr_str += ":" + std::to_string(ntohs(client_addr.sin_port));

        worker_threads.emplace_back(handle_worker, client_fd, addr_str);
        worker_threads.back().detach();
    }

    ::close(server_fd);

    // Brief sleep to let worker threads finish their final sends.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    log("MASTER  All tasks completed. Results in: " + spec.output_dir);
    return 0;
}
