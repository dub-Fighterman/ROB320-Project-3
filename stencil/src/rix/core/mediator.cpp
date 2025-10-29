#include "rix/core/mediator.hpp"

namespace rix {
namespace core {

Mediator::~Mediator() {}

bool Mediator::ok() const { return !shutdown_flag_; }

void Mediator::shutdown() { shutdown_flag_ = true; }

/**< TODO: Implement the spin_once method */
void Mediator::spin_once() {
    return;
}

/**< TODO: Implement the notify_subscribers method. */
void Mediator::notify_subscribers(const std::vector<rix::msg::mediator::SubInfo> &subscribers,
                                  const rix::msg::mediator::PubInfo &publisher) {
    return;
}

/**< TODO: Implement the notify_subscribers method. */
void Mediator::notify_subscribers(const rix::msg::mediator::SubInfo &subscriber,
                                  const std::vector<rix::msg::mediator::PubInfo> &publishers) {
    return;
}

/**< TODO: Implement the validate_topic_info method. */
bool Mediator::validate_topic_info(const rix::msg::mediator::TopicInfo &info) {
    return false;
}

void Mediator::send_status_message(std::shared_ptr<rix::ipc::interfaces::Connection> conn,
                                   rix::msg::mediator::Status status) {
    std::vector<uint8_t> buffer(status.size());
    size_t offset = 0;
    status.serialize(buffer.data(), offset);

    ssize_t bytes = conn->write(buffer.data(), buffer.size());
    if (bytes != buffer.size()) {
        rix::util::Log::warn << "Failed to write to rixhub." << std::endl;
    }
}

Mediator::Mediator(const rix::ipc::Endpoint &rixhub_endpoint, ServerFactory server_factory,
                   ClientFactory client_factory)
    : server_(server_factory(rixhub_endpoint)), client_factory_(client_factory), shutdown_flag_(false) {
    if (!server_->ok()) {
        shutdown();
    }
    rix::util::Log::init("rixhub");
}

}  // namespace core
}  // namespace rix