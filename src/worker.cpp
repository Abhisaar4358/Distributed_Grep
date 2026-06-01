/**
 * worker.cpp
 * ----------
 * Worker node for the Distributed Grep MapReduce system (Dean & Ghemawat,
 * OSDI '04, §3.1).
 *
 * Lifecycle:
 *   1. Connect to Master via TCP (MASTER_PORT).
 *   2. Send WORKER_READY.
 *   3. Receive a task assignment (MAP, REDUCE, WAIT, or DONE).
 *   4. Execute the task.
 *   5. Notify Master with TASK_DONE <id> <type>.
 *   6. Repeat from step 2.
 *
 * Fault-tolerance note (paper §3.3):
 *   If this process crashes mid-task, the Master will time-out the
 *   in-progress task and re-assign it to another (or restarted) worker.
 *   Completed MAP tasks need to be re-executed after failure because their
 *   output lives on local disk; completed REDUCE tasks do not (output goes
 *   to a shared filesystem).
 *
 * Usage:
 *   ./worker [master_host]
 *
 *   master_host – hostname / IP of the Master (default: 127.0.0.1)
 */

#include "mapreduce.h"

#include <iostream>
#include <sstream>
#include <string>
#include <map>
#include <thread>
#include <chrono>
#include <stdexcept>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <cstring>
#include <cerrno>

// Protocol parser

static std::map<std::string, std::string> parse_msg(const std::string& msg)
{
    std::map<std::string, std::string> kv;
    std::istringstream iss(msg);
    std::string line;
    while (std::getline(iss, line)) {
        if (line == "END" || line.empty()) continue;
        auto sp = line.find(' ');
        if (sp == std::string::npos) continue;
        kv[line.substr(0, sp)] = line.substr(sp + 1);
    }
    return kv;
}


// Task execution


static void run_map_task(int master_fd,
                          const std::map<std::string, std::string>& kv)
{
    int         task_id   = std::stoi(kv.at("TASK_ID"));
    std::string input     = kv.at("INPUT");
    std::string pattern   = kv.at("PATTERN");
    int         r_count   = std::stoi(kv.at("R_COUNT"));
    std::string inter_dir = kv.at("INTER_DIR");

    try {
        map_fn(input, pattern, r_count, inter_dir, task_id);
    } catch (const std::exception& e) {
        log("WORKER  MAP task=" + std::to_string(task_id)
            + " FAILED: " + e.what());
        return;  // Master will time out and re-assign.
    }

    // Notify master.
    std::string notif = "TASK_DONE " + std::to_string(task_id) + " MAP\nEND\n";
    if (!send_all(master_fd, notif)) {
        log("WORKER  Failed to notify master of MAP completion");
    }
}


static void run_reduce_task(int master_fd,
                             const std::map<std::string, std::string>& kv)
{
    int         task_id   = std::stoi(kv.at("TASK_ID"));
    int         partition = std::stoi(kv.at("PARTITION"));
    std::string inter_dir = kv.at("INTER_DIR");
    std::string output    = kv.at("OUTPUT");

    try {
        reduce_fn(partition, inter_dir, output);
    } catch (const std::exception& e) {
        log("WORKER  REDUCE task=" + std::to_string(task_id)
            + " FAILED: " + e.what());
        return;
    }

    std::string notif = "TASK_DONE " + std::to_string(task_id) + " REDUCE\nEND\n";
    if (!send_all(master_fd, notif)) {
        log("WORKER  Failed to notify master of REDUCE completion");
    }
}


// Connection helper


static int connect_to_master(const std::string& host)
{
    struct addrinfo hints{}, *res;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    std::string port_str = std::to_string(MASTER_PORT);
    int gai = ::getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
    if (gai != 0) {
        std::cerr << "getaddrinfo: " << gai_strerror(gai) << "\n";
        return -1;
    }

    int fd = ::socket(res->ai_family, res->ai_socktype, 0);
    if (fd < 0) { perror("socket"); freeaddrinfo(res); return -1; }

    if (::connect(fd, res->ai_addr, res->ai_addrlen) < 0) {
        perror("connect"); ::close(fd); freeaddrinfo(res); return -1;
    }
    freeaddrinfo(res);
    return fd;
}



int main(int argc, char* argv[])
{
    std::string master_host = (argc >= 2) ? argv[1] : "127.0.0.1";
    int worker_id = static_cast<int>(getpid()); // Use PID as worker identity.

    log("WORKER  pid=" + std::to_string(worker_id)
        + "  connecting to master " + master_host
        + ":" + std::to_string(MASTER_PORT));

    // Retry connection loop – the worker may start before the master.
    int master_fd = -1;
    for (int attempt = 0; attempt < 10; ++attempt) {
        master_fd = connect_to_master(master_host);
        if (master_fd >= 0) break;
        log("WORKER  Connection failed, retrying in 2 s…");
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
    if (master_fd < 0) {
        std::cerr << "WORKER  Could not connect to master after 10 attempts.\n";
        return 1;
    }

    log("WORKER  Connected to master");

    //  Main task loop 
    bool running = true;
    while (running) {
        // Send WORKER_READY (or re-send after task completion / wait).
        if (!send_all(master_fd, "WORKER_READY\nEND\n")) {
            log("WORKER  Master connection lost");
            break;
        }

        // Receive task assignment.
        std::string msg = recv_until_end(master_fd);
        if (msg.empty()) {
            log("WORKER  Master closed connection");
            break;
        }

        auto kv = parse_msg(msg);

        // Dispatch on task type.
        const std::string& ttype = kv.count("TASK_TYPE") ? kv["TASK_TYPE"] : "";

        if (ttype == "MAP") {
            run_map_task(master_fd, kv);

        } else if (ttype == "REDUCE") {
            run_reduce_task(master_fd, kv);

        } else if (ttype == "WAIT") {
            // Master has no task ready yet (map phase still running).
            log("WORKER  No task available, waiting…");
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

        } else if (ttype == "DONE") {
            log("WORKER  Received DONE signal – exiting cleanly");
            running = false;

        } else {
            log("WORKER  Unknown task type: '" + ttype + "', ignoring");
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    ::close(master_fd);
    log("WORKER  Shut down (pid=" + std::to_string(worker_id) + ")");
    return 0;
}
