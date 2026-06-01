#pragma once

#include <string>
#include <vector>
#include <functional>
#include <cstdint>

constexpr int MASTER_PORT = 9000;
constexpr int CONN_BACKLOG = 64;
constexpr size_t MAX_MSG_SIZE = 65536;

enum class TaskType : uint8_t
{
    NONE = 0,
    MAP = 1,
    REDUCE = 2,
    DONE = 3,
};

enum class TaskStatus : uint8_t
{
    IDLE = 0,
    IN_PROGRESS = 1,
    COMPLETED = 2,
};

// Task descriptors

struct MapTask
{
    int id;                 
    std::string input_file; 
    std::string pattern;    
    int r_count;            
    std::string inter_dir;  

    TaskStatus status = TaskStatus::IDLE;
    int worker_fd = -1;     
    time_t assigned_at = 0;  
};

struct ReduceTask
{
    int id;
    int partition;
    std::string inter_dir;
    std::string output_file;

    TaskStatus status = TaskStatus::IDLE;
    int worker_fd = -1;
    time_t assigned_at = 0;
};

struct JobSpec
{
    std::string pattern;
    std::vector<std::string> input_files;
    std::string output_dir;
    std::string inter_dir;
    int r_count;
};

// MapReduce function interfaces 

using EmitFn = std::function<void(const std::string &key, const std::string &value)>;

void map_fn(const std::string &input_file, const std::string &pattern, int r_count, const std::string &inter_dir, int task_id);

void reduce_fn(int partition, const std::string &inter_dir, const std::string &output_file);

// Utility helpers 

std::string inter_file_path(const std::string &inter_dir, int map_id, int reduce_id);

int partition_key(const std::string &key, int r_count);

bool send_all(int fd, const std::string &data);

std::string recv_until_end(int fd);

void log(const std::string &msg);
