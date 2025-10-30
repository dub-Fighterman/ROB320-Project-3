#include "rix/core/mediator.hpp"

namespace rix {
namespace core {

Mediator::~Mediator() {}

bool Mediator::ok() const { return !shutdown_flag_; }

void Mediator::shutdown() { shutdown_flag_ = true; }

/**< TODO: Implement the spin_once method */
void Mediator::spin_once() {
    if (shutdown_flag_ || !server_ || !server_->ok()) {
        return;
    }

    if (!server_->wait_for_accept(rix::util::Duration(0.0))) {
        return;
    }

    std::weak_ptr<rix::ipc::interfaces::Connection> wconn;
    if (!server_->accept(wconn)){
        return;
    }

    auto conn = wconn.lock();
    if (!conn) {
        return;
    }

    rix::msg::mediator::Operation op;
    std::vector<uint8_t> hdr(op.size());
    ssize_t rd = conn->read(hdr.data(), hdr.size());
    if (rd != static_cast<ssize_t>(hdr.size())) {
        return;
    }

    size_t offset = 0;
    if (!op.deserialize(hdr.data(), hdr.size(), offset)) {
        return;
    }

    std::vector<uint8_t> payload(op.len);
    ssize_t rd2 = conn->read(payload.data(), payload.size());
    if (rd2 != static_cast<ssize_t>(payload.size())) {
        return;
    }

    switch (static_cast<OPCODE>(op.opcode)) {
        case OPCODE::NODE_REGISTER: {
            rix::msg::mediator::NodeInfo node;
            size_t poff = 0;
            if (!node.deserialize(payload.data(), payload.size(), poff)) {
                return;
            }
            {
                std::lock_guard<std::mutex> lock(mutex);
                nodes_[node.id] = node;
            }

            rix::msg::mediator::Status status;
            status.error = 0;
            std::vector<uint8_t> buf(status.size());
            size_t off = 0;
            status.serialize(buf.data(), off);
            conn->write(buf.data(), buf.size());
            break;
        }

        case OPCODE::NODE_DEREGISTER: {
            rix::msg::mediator::NodeInfo node;
            size_t poff = 0;
            if (!node.deserialize(payload.data(), payload.size(), poff)) {
                return;
            }
            {
                std::lock_guard<std::mutex> lock(mutex_);
                nodes_.erase(node.id);
            }
            break;
        }

        case OPCODE::PUB_REGISTER: {
            rix::msg::mediator::PubInfo pub;
            size_t poff = 0;
            if (!pub.deserialize(payload.data(), payload.size(), poff)) {
                return;
            }
            bool valid = validate_topic_info(pub.topic_info);
            if (valid) {
                std::lock_guard<std::mutex> lock(mutex_);
                publishers_[pub.topic_info.id].push_back(pub);
            }
            rix::msg::mediator::Status status;
            status.error = valid ? 0 : 1;
            std::vector<uint8_t> buf(status.size());
            size_t off = 0;
            status.serialize(buf.data(), off);
            conn->write(buf.data(), buf.size());
            if (valid) {
                notify_subscribers(pub.topic_info.id);
            }
            break;
        }

        case OPCODE::PUB_DEREGISTER: {
            rix::msg::mediator::PubInfo pub;
            size_t poff = 0;
            if (!pub.deserialize(payload.data(), payload.size(), poff)) return;

            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = publishers_.find(pub.topic_info.id);
                if (it != publishers_.end()) {
                    auto &vec = it->second;
                    vec.erase(std::remove_if(vec.begin(), vec.end(),
                                             [&](const auto &p) { return p.id == pub.id; }),
                              vec.end());
                }
            }
            break;
        }

        case OPCODE::SUB_REGISTER: {
            rix::msg::mediator::SubInfo sub;
            size_t poff = 0;
            if (!sub.deserialize(payload.data(), payload.size(), poff)) return;
            bool valid = validate_topic_info(sub.topic_info);
            if (valid) {
                std::lock_guard<std::mutex> lock(mutex_);
                subscribers_[sub.topic_info.id].push_back(sub);
            }
            rix::msg::mediator::Status status;
            status.error = valid ? 0 : 1;
            std::vector<uint8_t> buf(status.size());
            size_t off = 0;
            status.serialize(buf.data(), off);
            conn->write(buf.data(), buf.size());
            if (valid) {
                notify_subscribers(sub.topic_info.id, sub.endpoint);
            }
            break;
        }

        case OPCODE::SUB_DEREGISTER: {
            rix::msg::mediator::SubInfo sub;
            size_t poff = 0;
            if (!sub.deserialize(payload.data(), payload.size(), poff)) return;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                auto it = subscribers_.find(sub.topic_info.id);
                if (it != subscribers_.end()) {
                    auto &vec = it->second;
                    vec.erase(std::remove_if(vec.begin(), vec.end(),
                                             [&](const auto &s) { return s.id == sub.id; }),
                              vec.end());
                }
            }
            break;
        }
        default: {
            rix::util::Log::warn << "Mediator: Unknown opcode " << op.opcode << std::endl;
            break;
        }
    }
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
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = topics_.find(topic.id);
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