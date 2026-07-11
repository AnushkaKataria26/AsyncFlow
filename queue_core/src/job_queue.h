#pragma once

#include <string>
#include <deque>
#include <unordered_map>
#include <chrono>
#include <mutex>
#include <optional>
#include <utility>

class JobQueue {
public:
    JobQueue() = default;

    // Enqueue a job_id. Idempotent: does nothing if already pending or leased.
    void enqueue(const std::string& job_id);

    // Sweeps expired leases back to the queue, then pops the front job if any.
    // Returns the job_id and leases it for lease_duration_seconds.
    std::optional<std::string> dequeue_with_lease(int lease_duration_seconds);

    // Acknowledge a completed job. Removes it from leased_jobs.
    // Returns true if it was found in leased_jobs, false otherwise.
    bool acknowledge(const std::string& job_id);

    // Requeue a currently leased job back to the front of the queue.
    // Returns true if successful, false if the job was not in leased_jobs.
    bool requeue(const std::string& job_id);

    // Return the number of {pending, leased} jobs.
    std::pair<size_t, size_t> size();

    // Check if a job_id is either pending or leased.
    bool contains(const std::string& job_id);

private:
    // Move expired leases back to the front of the queue
    void sweep_expired_leases(std::chrono::steady_clock::time_point now);

    std::deque<std::string> queue_;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point> leased_jobs_;
    std::mutex mutex_;
};
