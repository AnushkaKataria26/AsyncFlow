#include "job_queue.h"
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <memory>
#include <cstring>
#include <cstdlib>
#include <cctype>
#include <unordered_map>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <csignal>

// Global shutdown flag
std::atomic<bool> running{true};
int server_fd = -1;

void handle_signal(int sig) {
    std::cout << "\n[SERVER] Received signal " << sig << ". Shutting down gracefully..." << std::endl;
    running = false;
    if (server_fd != -1) {
        // Close the server socket to unblock accept()
        close(server_fd);
        server_fd = -1;
    }
}

// Helper to get current timestamp string
std::string current_time_str() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    char buf[100] = {0};
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now_time));
    return std::string(buf);
}

// Log with timestamp
void log_info(const std::string& msg) {
    std::cout << "[" << current_time_str() << "] " << msg << std::endl;
}

// Token registry for worker authentication
// Note: Queue server restart loses all token state (in-memory registry).
// Workers must re-register on reconnect. Persistence of tokens would require a DB-backed registry in production.
std::unordered_map<std::string, std::string> token_to_worker_id;
std::mutex token_mutex;

// Token validation function
// Tokens are opaque strings (UUIDs from Python side) - no cryptographic verification in C++, just registry lookup.
// In production, tokens would be signed JWTs verified with a shared secret. For this project, registry lookup is sufficient.
bool validate_token(const std::string& token) {
    if (token.empty()) return false;
    std::lock_guard<std::mutex> lock(token_mutex);
    return token_to_worker_id.find(token) != token_to_worker_id.end();
}

// Validate if a job ID contains whitespace (we assume UUIDs or similar tokens without spaces)
bool has_whitespace(const std::string& str) {
    for (char c : str) {
        if (std::isspace(static_cast<unsigned char>(c))) return true;
    }
    return false;
}

// Handle a single client connection
void handle_client(int client_sock, std::shared_ptr<JobQueue> job_queue) {
    char buffer[256];
    std::string incomplete_line = "";
    bool discard_until_newline = false;

    while (running) {
        memset(buffer, 0, sizeof(buffer));
        ssize_t bytes_read = read(client_sock, buffer, sizeof(buffer) - 1);

        if (bytes_read <= 0) {
            // Disconnect or error
            break;
        }

        std::string data(buffer, bytes_read);
        size_t pos = 0;

        while ((pos = data.find('\n')) != std::string::npos) {
            if (discard_until_newline) {
                discard_until_newline = false;
                data.erase(0, pos + 1);
                continue;
            }

            if (incomplete_line.length() + pos > 256) {
                std::string err_msg = "ERROR line too long\n";
                write(client_sock, err_msg.c_str(), err_msg.length());
                log_info("[SEND] ERROR line too long");
                incomplete_line = "";
                data.erase(0, pos + 1);
                continue;
            }

            std::string line = incomplete_line + data.substr(0, pos);
            // Handle carriage return from windows/telnet if present
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }

            data.erase(0, pos + 1);
            incomplete_line = "";

            if (line.empty()) continue;

            log_info("[RECV] " + line);

            std::istringstream iss(line);
            std::string cmd;
            iss >> cmd;

            std::string response = "ERROR unknown command\n";

            try {
                if (cmd == "REGISTER") {
                    std::string worker_id, auth_token;
                    iss >> worker_id >> auth_token;
                    if (worker_id.empty() || auth_token.empty()) {
                        response = "ERROR missing worker_id or auth_token\n";
                    } else {
                        std::lock_guard<std::mutex> lock(token_mutex);
                        bool already_registered = false;
                        for (const auto& pair : token_to_worker_id) {
                            if (pair.second == worker_id) {
                                if (pair.first == auth_token) {
                                    response = "OK\n";
                                } else {
                                    response = "ERROR worker_id already registered\n";
                                }
                                already_registered = true;
                                break;
                            }
                        }
                        if (!already_registered) {
                            token_to_worker_id[auth_token] = worker_id;
                            response = "OK\n";
                        }
                    }
                } else if (cmd == "DEREGISTER") {
                    std::string worker_id, auth_token;
                    iss >> worker_id >> auth_token;
                    if (worker_id.empty() || auth_token.empty()) {
                        response = "ERROR missing worker_id or auth_token\n";
                    } else {
                        std::lock_guard<std::mutex> lock(token_mutex);
                        auto it = token_to_worker_id.find(auth_token);
                        if (it != token_to_worker_id.end() && it->second == worker_id) {
                            token_to_worker_id.erase(it);
                            response = "OK\n";
                        } else {
                            response = "ERROR invalid token or worker_id mismatch\n";
                        }
                    }
                } else if (cmd == "ENQUEUE") {
                    std::string job_id, auth_token;
                    iss >> job_id >> auth_token;
                    if (job_id.empty() || auth_token.empty()) {
                        response = "ERROR missing job_id or auth_token\n";
                    } else if (!validate_token(auth_token)) {
                        response = "AUTH_ERROR\n";
                    } else {
                        if (job_queue->contains(job_id)) {
                            response = "DUPLICATE\n";
                        } else {
                            job_queue->enqueue(job_id);
                            response = "OK\n";
                        }
                    }
                } else if (cmd == "DEQUEUE") {
                    std::string lease_str, auth_token;
                    iss >> lease_str >> auth_token;
                    if (lease_str.empty() || auth_token.empty()) {
                        response = "ERROR missing lease duration or auth_token\n";
                    } else if (!validate_token(auth_token)) {
                        response = "AUTH_ERROR\n";
                    } else {
                        try {
                            int lease = std::stoi(lease_str);
                            if (lease <= 0) {
                                response = "ERROR invalid lease duration\n";
                            } else {
                                auto job = job_queue->dequeue_with_lease(lease);
                                if (job.has_value()) {
                                    response = "JOB " + job.value() + "\n";
                                } else {
                                    response = "EMPTY\n";
                                }
                            }
                        } catch (std::invalid_argument&) {
                            response = "ERROR invalid lease duration format\n";
                        }
                    }
                } else if (cmd == "ACK") {
                    std::string job_id, auth_token;
                    iss >> job_id >> auth_token;
                    if (job_id.empty() || auth_token.empty()) {
                        response = "ERROR missing job_id or auth_token\n";
                    } else if (!validate_token(auth_token)) {
                        response = "AUTH_ERROR\n";
                    } else {
                        if (job_queue->acknowledge(job_id)) {
                            response = "OK\n";
                        } else {
                            response = "NOT_FOUND\n";
                        }
                    }
                } else if (cmd == "REQUEUE") {
                    std::string job_id, auth_token;
                    iss >> job_id >> auth_token;
                    if (job_id.empty() || auth_token.empty()) {
                        response = "ERROR missing job_id or auth_token\n";
                    } else if (!validate_token(auth_token)) {
                        response = "AUTH_ERROR\n";
                    } else {
                        if (job_queue->requeue(job_id)) {
                            response = "OK\n";
                        } else {
                            response = "NOT_FOUND\n";
                        }
                    }

                } else if (cmd == "STATUS") {
                    auto counts = job_queue->size();
                    response = "PENDING " + std::to_string(counts.first) + " LEASED " + std::to_string(counts.second) + "\n";
                } else if (cmd == "PING") {
                    response = "PONG\n";
                }
            } catch (...) {
                response = "ERROR unexpected error processing command\n";
            }

            log_info("[SEND] " + response.substr(0, response.length() - 1)); // Log without the newline
            write(client_sock, response.c_str(), response.length());
        }
        
        if (discard_until_newline) continue;

        incomplete_line += data;
        if (incomplete_line.length() > 256) {
            std::string err_msg = "ERROR line too long\n";
            write(client_sock, err_msg.c_str(), err_msg.length());
            log_info("[SEND] ERROR line too long");
            incomplete_line = "";
            discard_until_newline = true;
        }
    }

    close(client_sock);
}

int main(int argc, char* argv[]) {
    int port = 9000;
    
    // Check environment variable
    if (const char* env_port = std::getenv("QUEUE_PORT")) {
        port = std::atoi(env_port);
    }
    
    // Check command-line arg (overrides env var)
    if (argc > 1) {
        port = std::atoi(argv[1]);
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    // Ignore SIGPIPE to prevent crash on writing to a closed socket
    std::signal(SIGPIPE, SIG_IGN);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == 0) {
        std::cerr << "Socket creation failed" << std::endl;
        return 1;
    }

    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        std::cerr << "setsockopt failed" << std::endl;
        return 1;
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        std::cerr << "Bind failed on port " << port << std::endl;
        return 1;
    }

    if (listen(server_fd, 10) < 0) {
        std::cerr << "Listen failed" << std::endl;
        return 1;
    }

    log_info("Queue server listening on port " + std::to_string(port));

    auto job_queue = std::make_shared<JobQueue>();
    std::vector<std::thread> client_threads;

    while (running) {
        int client_sock = accept(server_fd, nullptr, nullptr);
        
        if (client_sock < 0) {
            if (running) {
                std::cerr << "Accept failed" << std::endl;
            }
            break;
        }

        client_threads.emplace_back(std::thread(handle_client, client_sock, job_queue));
    }

    log_info("Waiting for client threads to finish...");
    for (auto& t : client_threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    
    auto final_counts = job_queue->size();
    log_info("Server shutdown complete. Final state - PENDING: " + 
             std::to_string(final_counts.first) + ", LEASED: " + 
             std::to_string(final_counts.second));

    return 0;
}
