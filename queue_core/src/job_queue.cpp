#include "job_queue.h"
#include <algorithm>
#include <iostream>

void JobQueue::sweep_expired_leases(std::chrono::steady_clock::time_point now) {
    // Note: Assumes mutex_ is already locked by the caller.
    for (auto it = leased_jobs_.begin(); it != leased_jobs_.end(); ) {
        if (it->second <= now) {
            // Lease expired, move back to front of the queue
            queue_.push_front(it->first);
            // std::cout << "Lease expired for job: " << it->first << ", requeued." << std::endl;
            it = leased_jobs_.erase(it);
        } else {
            ++it;
        }
    }
}

void JobQueue::enqueue(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check if already leased
    if (leased_jobs_.find(job_id) != leased_jobs_.end()) {
        std::cerr << "WARNING: Job " << job_id << " is already leased. Ignoring enqueue." << std::endl;
        return;
    }
    
    // Check if already in queue
    if (std::find(queue_.begin(), queue_.end(), job_id) != queue_.end()) {
        std::cerr << "WARNING: Job " << job_id << " is already in queue. Ignoring enqueue." << std::endl;
        return;
    }
    
    queue_.push_back(job_id);
}

std::optional<std::string> JobQueue::dequeue_with_lease(int lease_duration_seconds) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    sweep_expired_leases(now);
    
    if (queue_.empty()) {
        return std::nullopt;
    }
    
    std::string job_id = queue_.front();
    queue_.pop_front();
    
    auto expiry_time = now + std::chrono::seconds(lease_duration_seconds);
    leased_jobs_[job_id] = expiry_time;
    
    return job_id;
}

bool JobQueue::acknowledge(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Note: A worker might ACK after its lease already expired and the job was reassigned.
    // In this simple version, we don't track *which* worker holds the lease, so we just remove
    // if present. This is a known limitation.
    auto it = leased_jobs_.find(job_id);
    if (it != leased_jobs_.end()) {
        leased_jobs_.erase(it);
        return true;
    }
    return false;
}

bool JobQueue::requeue(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = leased_jobs_.find(job_id);
    if (it != leased_jobs_.end()) {
        queue_.push_front(job_id);
        leased_jobs_.erase(it);
        return true;
    }
    return false;
}

std::pair<size_t, size_t> JobQueue::size() {
    std::lock_guard<std::mutex> lock(mutex_);
    // Also a good opportunity to sweep if we wanted accurate "pending" count, 
    // but typically size is an instant snapshot, we'll sweep so it's accurate.
    sweep_expired_leases(std::chrono::steady_clock::now());
    return {queue_.size(), leased_jobs_.size()};
}

bool JobQueue::contains(const std::string& job_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (leased_jobs_.find(job_id) != leased_jobs_.end()) {
        return true;
    }
    return std::find(queue_.begin(), queue_.end(), job_id) != queue_.end();
}
