#include "rix/core/mediator.hpp"

namespace rix {
namespace core {

Mediator::~Mediator() {}

bool Mediator::ok() const { return !shutdown_flag_; }

void Mediator::shutdown() { shutdown_flag_ = true; }

/**< TODO: Implement the spin_once method */
void Mediator::spin_once() {
    if (shutdown_flag_.load() || !server_ || !server_->ok()) return;

    // Wait for an incoming connection from a node/publisher/subscriber
    if (!server_->wait_for_accept(rix::util::Duration(0.0))) return;

    std::weak_ptr<rix::ipc::interfaces::Connection> wconn;
    if (!server_->accept(wconn)) return;
    auto conn = wconn.lock();
    if (!conn) return;

    // Read operation header
    rix::msg::mediator::Operation op;
    std::vector<uint8_t> hdr(op.size());
    ssize_t hbytes = conn->read(hdr.data(), hdr.size());
    size_t hoff = 0;

    if (hbytes != static_cast<ssize_t>(hdr.size()) || !op.deserialize(hdr.data(), hbytes, hoff)) {
        rix::msg::mediator::Status status;
        status.error = 1;
        send_status_message(conn, status);
        return;
    }

    std::vector<uint8_t> payload(op.len);
    ssize_t pbytes = conn->read(payload.data(), payload.size());
    if (pbytes != static_cast<ssize_t>(payload.size())) {
        rix::msg::mediator::Status status;
        status.error = 1;
        send_status_message(conn, status);
        return;
    }
    size_t poff = 0;

    switch (op.opcode) {
        case OPCODE::NODE_REGISTER: {
            rix::msg::mediator::NodeInfo info;
            if (!info.deserialize(payload.data(), payload.size(), poff)) {
                rix::msg::mediator::Status status;
                status.error = 1;
                send_status_message(conn, status);
                return;
            }
            nodes_[info.id] = info;
            rix::util::Log::info << "[rixhub] Registered node \"" << info.name << "\"." << std::endl;
            rix::msg::mediator::Status status;
            status.error = 0;
            send_status_message(conn, status);
            break;
        }

        case OPCODE::NODE_DEREGISTER: {
            rix::msg::mediator::NodeInfo info;
            if (info.deserialize(payload.data(), payload.size(), poff)) {
                nodes_.erase(info.id);
                rix::util::Log::info << "[rixhub] Deregistered node \"" << info.name << "\"." << std::endl;
            }
            break;
        }

        case OPCODE::PUB_REGISTER: {
            rix::msg::mediator::PubInfo info;
            if (!info.deserialize(payload.data(), payload.size(), poff)) {
                rix::msg::mediator::Status status;
                status.error = 1;
                send_status_message(conn, status);
                return;
            }
            if (!validate_topic_info(info.topic_info)) {
                rix::util::Log::warn << "[rixhub] Reject publisher (topic hash mismatch): " << info.topic_info.name << std::endl;
                rix::msg::mediator::Status status;
                status.error = 1;
                send_status_message(conn, status);
                return;
            }

            publishers_[info.id] = info;
            rix::util::Log::info << "[rixhub] Registered publisher on \"" << info.topic_info.name << "\"." << std::endl;
            rix::msg::mediator::Status status;
            status.error = 0;
            send_status_message(conn, status);

            // Notify all subscribers of this topic
            std::vector<rix::msg::mediator::SubInfo> subs;
            for (const auto &kv : subscribers_) {
                if (kv.second.topic_info.name == info.topic_info.name)
                    subs.push_back(kv.second);
            }
            if (!subs.empty())
                notify_subscribers(subs, info);
            break;
        }

        case OPCODE::PUB_DEREGISTER: {
            rix::msg::mediator::PubInfo info;
            if (info.deserialize(payload.data(), payload.size(), poff)) {
                publishers_.erase(info.id);
                rix::util::Log::info << "[rixhub] Deregistered publisher on \""
                                     << info.topic_info.name << "\"." << std::endl;
            }
            break;
        }

        case OPCODE::SUB_REGISTER: {
            rix::msg::mediator::SubInfo info;
            if (!info.deserialize(payload.data(), payload.size(), poff)) {
                rix::msg::mediator::Status status;
                status.error = 1;
                send_status_message(conn, status);
                return;
            }
            if (!validate_topic_info(info.topic_info)) {
                rix::util::Log::warn << "[rixhub] Reject subscriber (topic hash mismatch): " << info.topic_info.name << std::endl;
                rix::msg::mediator::Status status;
                status.error = 1;
                send_status_message(conn, status);
                return;
            }

            subscribers_[info.id] = info;
            rix::util::Log::info << "[rixhub] Registered subscriber on \"" << info.topic_info.name << "\"." << std::endl;
            rix::msg::mediator::Status status;
            status.error = 0;
            send_status_message(conn, status);

            // Notify this subscriber of all current publishers on the same topic
            std::vector<rix::msg::mediator::PubInfo> pubs;
            for (const auto &kv : publishers_) {
                if (kv.second.topic_info.name == info.topic_info.name)
                    pubs.push_back(kv.second);
            }
            if (!pubs.empty())
                notify_subscribers(info, pubs);
            break;
        }

        case OPCODE::SUB_DEREGISTER: {
            rix::msg::mediator::SubInfo info;
            if (info.deserialize(payload.data(), payload.size(), poff)) {
                subscribers_.erase(info.id);
                rix::util::Log::info << "[rixhub] Deregistered subscriber on \""
                                     << info.topic_info.name << "\"." << std::endl;
            }
            break;
        }

        default: {
            rix::util::Log::warn << "[rixhub] Unknown opcode received: " << op.opcode << std::endl;
            rix::msg::mediator::Status status;
            status.error = 1;
            send_status_message(conn, status);
            break;
        }
    }
}

/**< TODO: Implement the notify_subscribers method. */
void Mediator::notify_subscribers(const std::vector<rix::msg::mediator::SubInfo> &subscribers,
                                  const rix::msg::mediator::PubInfo &publisher) {
    if (subscribers.empty()) {
        return;
    }

    rix::msg::mediator::SubNotify notify;
    notify.publishers.clear();
    notify.publishers.push_back(publisher);

    for (const auto &sub : subscribers) {
        auto client = client_factory_ ? client_factory_() : nullptr;
        if (!client) {
            continue;
        }

        rix::ipc::Endpoint ep(sub.endpoint.address, sub.endpoint.port);

        (void)send_message_with_opcode_no_response(client, notify, OPCODE::SUB_NOTIFY, ep);
    }
}

/**< TODO: Implement the notify_subscribers method. */
void Mediator::notify_subscribers(const rix::msg::mediator::SubInfo &sub,
                                  const std::vector<rix::msg::mediator::PubInfo> &pubs) {
    if (pubs.empty()) {
        return;
    }

    rix::msg::mediator::SubNotify notify;
    notify.publishers = pubs;

    auto client = client_factory_ ? client_factory_() : nullptr;
    if (!client) {
        return;
    }

    /**for (const auto &pub : publishers) {
        auto client = client_factory_ ? client_factory_() : nullptr;
        if (!client) {
            continue;
        }

        rix::ipc::Endpoint ep(pub.endpoint.address, pub.endpoint.port);

        (void)send_message_with_opcode_no_response(client, notify, OPCODE::SUB_NOTIFY, ep);
    }**/
    rix::ipc::Endpoint ep(sub.endpoint.address, sub.endpoint.port);
    (void)send_message_with_opcode_no_response(client, notify, OPCODE::SUB_NOTIFY, ep);
}

/**< TODO: Implement the validate_topic_info method. */
bool Mediator::validate_topic_info(const rix::msg::mediator::TopicInfo &info) {
    auto it = topic_hashes_.find(info.name);
    if (it == topic_hashes_.end()) {
        topic_hashes_[info.name] = info.message_hash;
        return true;
    }
    return it->second == info.message_hash;
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