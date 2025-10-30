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
    auto client = factory_();
    if (client && client->connect(rixhub_endpoint_)) {
        rix::msg::mediator::Operation op;
        op.opcode = static_cast<uint16_t>(OPCODE::SUB_REGISTER);
        op.len = static_cast<uint32_t>(info_.size());

        std::vector<uint8_t> buf(op.size() + info_.size());
        size_t offset = 0;
        op.serialize(buf.data(), offset);
        info_.serialize(buf.data(), offset);
        (void)client->write(buf.data(), buf.size());

        rix::msg::mediator::Status status;
        std::vector<uint8_t> sbuf(status.size());
        ssize_t rd = client->read(sbuf.data(), sbuf.size());

        if (rd == (ssize_t)sbuf.size()) {
            size_t soff = 0;
            (void)status.deserialize(sbuf.data(), sbuf.size(), soff);
            if (status.error != 0) {
                rix::util::Log::warn << "Subscriber registration error: " << (int)status.error << std::endl;
            }
        }
    }
    else {
        rix::util::Log::warn << "Subscriber failed to connect to rixhub for registration." << std::endl;
    }
}

Subscriber::~Subscriber() {
    //shutdown();

    /**< TODO: Deregister the subscriber with the mediator */
    auto client = factory_();
    if (client && client->connect(rixhub_endpoint_)) {
        rix::msg::mediator::Operation op;
        op.opcode = static_cast<uint16_t>(OPCODE::SUB_DEREGISTER);
        op.len = static_cast<uint32_t>(info_.size());

        std::vector<uint8_t> buf(op.size() + info_.size());
        size_t offset = 0;
        op.serialize(buf.data(), offset);
        info_.serialize(buf.data(), offset);
        (void)client->write(buf.data(), buf.size());
    }
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
    if (shutdown_flag_) return;
    /**for (;;) {
        std::weak_ptr<rix::ipc::interfaces::Connection> wconn;
        if (!server_->accept(wconn)) {
            break;
        }
        if (server_->accept(wconn)) {
            auto conn = wconn.lock();
            if (conn) {
                rix::msg::mediator::Operation op;
                std::vector<uint8_t> hdr(op.size());
                ssize_t rd = conn->read(hdr.data(), hdr.size());
                if (rd == static_cast<ssize_t>(hdr.size())) {
                    size_t offset = 0;
                    if (op.deserialize(hdr.data(), hdr.size(), offset)) {
                        if (op.deserialize(hdr.data(), hdr.size(), offset) &&
                        op.opcode == static_cast<uint16_t>(OPCODE::SUB_NOTIFY) &&
                        op.len > 0) {
                            std::vector<uint8_t> payload(op.len);
                            ssize_t rd2 = conn->read(payload.data(), payload.size());
                            if (rd2 == (ssize_t)payload.size()) {
                                rix::msg::mediator::SubNotify notify;
                                size_t poff = 0;
                                if (notify.deserialize(payload.data(), payload.size(), poff)) {
                                    for (const auto &pub : notify.publishers) {
                                        auto cli = factory_();
                                        if (!cli) {
                                            continue;
                                        }
                                        cli->set_nonblocking(true);
                                        rix::ipc::Endpoint ep(pub.endpoint.address, pub.endpoint.port);
                                        if (cli->connect(ep)) {
                                            std::lock_guard<std::mutex> guard(callback_mutex_);
                                            clients_[pub.id] = cli;
                                        }
                                    }
                                }
                                else {
                                    rix::util::Log::warn << "Subscriber failed to deserialize SubNotify." << std::endl;
                                }
                            }
                        }
                    }
                }
            }
        }
    }**/
    for (;;) {
        std::weak_ptr<rix::ipc::interfaces::Connection> wconn;
        if (!server_->accept(wconn)) {
            break;
        }
        if (auto conn = wconn.lock()) {
            rix::msg::mediator::Operation op;
            std::vector<uint8_t> hdr(op.size());
            ssize_t rd = conn->read(hdr.data(), hdr.size());
            if (rd != static_cast<ssize_t>(hdr.size())) {
                continue;
            }

            size_t off = 0;
            if (!op.deserialize(hdr.data(), hdr.size(), off)) {
                continue;
            }
            if (op.opcode != static_cast<uint16_t>(OPCODE::SUB_NOTIFY) || op.len == 0) {
                continue;
            }

            std::vector<uint8_t> payload(op.len);
            ssize_t rd2 = conn->read(payload.data(), payload.size());
            if (rd2 != static_cast<ssize_t>(payload.size())) {
                continue;
            }

            rix::msg::mediator::SubNotify notify;
            size_t poff = 0;
            if (!notify.deserialize(payload.data(), payload.size(), poff)) {
                continue;
            }

            for (const auto &pub : notify.publishers) {
                auto cli = factory_();
                if (!cli) {
                    continue;
                }
                cli->set_nonblocking(true);
                rix::ipc::Endpoint ep(pub.endpoint.address, pub.endpoint.port);
                if (cli->connect(ep)) {
                    std::lock_guard<std::mutex> guard(callback_mutex_);
                    clients_[pub.id] = cli;
                }
            }
        }
    }

    std::vector<std::shared_ptr<rix::ipc::interfaces::Client>> local_clients;
    {
        std::lock_guard<std::mutex> guard(callback_mutex_);
        local_clients.reserve(clients_.size());
        /**for (auto it = clients_.begin(); it != clients_.end();) {
            if (auto sp = it->lock()) {
                local_clients.push_back(sp);
                ++it;
            }
            else {
                it = clients_.erase(it);
            }
        }**/
        for (const auto &kv : clients_) {
            local_clients.push_back(kv.second);
        }
    }

    for (auto &cli : local_clients) {
        if (!cli) {
            continue;
        }
        rix::msg::standard::UInt32 sz;
        std::vector<uint8_t> lenbuf(sz.size());
        ssize_t rd = cli->read(lenbuf.data(), lenbuf.size());
        if (rd != static_cast<ssize_t>(lenbuf.size())) {
            continue;
        }
        size_t off = 0;
        if (!sz.deserialize(lenbuf.data(), lenbuf.size(), off) || sz.data == 0) {
            continue;
        }

        std::vector<uint8_t> payload(sz.data);
        ssize_t rd2 = cli->read(payload.data(), payload.size());
        if (rd2 != static_cast<ssize_t>(payload.size())) {
            continue;
        }

        SerializedCallback callback;
        {
            std::lock_guard<std::mutex> guard(callback_mutex_);
            callback = callback_;
        }
        if (callback) {
            callback(payload.data(), payload.size());
        }
    }
}

}  // namespace core
}  // namespace rix