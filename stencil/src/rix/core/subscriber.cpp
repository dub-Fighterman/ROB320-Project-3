#include "rix/core/subscriber.hpp"

namespace rix {
namespace core {

Subscriber::Subscriber(const rix::msg::mediator::SubInfo &info, std::shared_ptr<rix::ipc::interfaces::Server> server,
                       ClientFactory factory, const rix::ipc::Endpoint &rixhub_endpoint)
    : info_(info), server_(server), factory_(factory), callback_(nullptr), rixhub_endpoint_(rixhub_endpoint) {
    // Ensure server was intitialized properly
    if (!server_->ok()) {
        shutdown();
        return;
    }

    /**< TODO: Register the subscriber with the mediator */
}

Subscriber::~Subscriber() {
    shutdown();

    /**< TODO: Deregister the subscriber with the mediator */
}

bool Subscriber::ok() const { return !shutdown_flag_; }

void Subscriber::shutdown() { shutdown_flag_ = true; }

Subscriber::SerializedCallback Subscriber::get_callback() const { return callback_; }

size_t Subscriber::get_publisher_count() const {
    std::lock_guard<std::mutex> guard(callback_mutex_);
    return clients_.size();
}

/**< TODO: Implement the spin_once method */
void Subscriber::spin_once() {
    return;
}

}  // namespace core
}  // namespace rix