#include "rix/core/publisher.hpp"

namespace rix {
namespace core {

Publisher::Publisher(const rix::msg::mediator::PubInfo &info, std::shared_ptr<rix::ipc::interfaces::Server> server,
                     ClientFactory factory, rix::ipc::Endpoint rixhub_endpoint)
    : info_(info), server_(server), shutdown_flag_(false), factory_(factory), rixhub_endpoint_(rixhub_endpoint) {
    // Ensure server was intitialized properly
    if (!server_->ok()) {
        rix::util::Log::error << "Server invalid!" << std::endl;
        shutdown();
        return;
    }

    /**< TODO: Register the publisher with the mediator */
    bool registered = true;
    if (factory_) {
        auto client = factory_();
        registered = send_message_with_opcode(client, info_, OPCODE::PUB_REGISTER, rixhub_endpoint_);
    }
    if (!registered) {
        rix::util::Log::warn << "Failed to register publisher with rixhub." << std::endl;
        shutdown_flag_.store(true);
        shutdown();
        return;
    }
}

Publisher::~Publisher() {
    shutdown();
    /**< TODO: Deregister the publisher with the mediator */
    if (factory_) {
        auto client = factory_();
        (void)send_message_with_opcode_no_response(client, info_, OPCODE::PUB_DEREGISTER, rixhub_endpoint_);
    }
}

bool Publisher::ok() const { return !shutdown_flag_; }

void Publisher::shutdown() { shutdown_flag_ = true; }

/**< TODO: Implement the publish method */
void Publisher::publish(const rix::msg::Message &msg) {
    if (shutdown_flag_.load()) {
        return;
    }
    
    rix::msg::standard::UInt32 size_prefix;
    size_prefix.data = static_cast<uint32_t>(msg.size());
    std::vector<uint8_t> buffer(size_prefix.size() + msg.size());
    size_t offset = 0;
    size_prefix.serialize(buffer.data(), offset);
    msg.serialize(buffer.data(), offset);

    std::lock_guard<std::mutex> guard(connections_mutex_);
    for (auto it = connections_.begin(); it != connections_.end();) {
        auto conn = it->lock();
        if (!conn) {
            it = connections_.erase(it);
            continue;
        }

        ssize_t bytes = conn->write(buffer.data(), buffer.size());
        if (bytes != static_cast<ssize_t>(buffer.size())) {
            rix::util::Log::warn << "Publisher failed to write full message; dropping connection." << std::endl;
            it = connections_.erase(it);
            continue;
        }

        ++it;
    }
}

size_t Publisher::get_subscriber_count() const {
    std::lock_guard<std::mutex> guard(connections_mutex_);
    return connections_.size();
}

/**< TODO: Implement the spin_once method */
void Publisher::spin_once() {
    // Check to see if a subscriber has made a connection
    if (!server_->wait_for_accept(rix::util::Duration(0.0))) {
        return;
    }

    // Accept a connection from a subscriber
    std::weak_ptr<rix::ipc::interfaces::Connection> conn;
    if (!server_->accept(conn)) {
        return;
    }

    // Store the connection
    std::lock_guard<std::mutex> guard(connections_mutex_);
    connections_.insert(conn);
}

}  // namespace core
}  // namespace rix