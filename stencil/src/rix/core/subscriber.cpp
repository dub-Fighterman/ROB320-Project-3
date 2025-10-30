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
    bool registered = true;
    if (factory_) {
        auto client = factory_();
        registered = send_message_with_opcode(client, info_, OPCODE::SUB_REGISTER, rixhub_endpoint_);
    }
    if (!registered) {
        rix::util::Log::warn << "Failed to register subscriber with rixhub." << std::endl;
        shutdown_flag_.store(true);
        shutdown();
        return;
    }
}

Subscriber::~Subscriber() {
    shutdown();

    /**< TODO: Deregister the subscriber with the mediator */
    if (factory_) {
        auto client = factory_();
        (void)send_message_with_opcode_no_response(client, info_, OPCODE::SUB_DEREGISTER, rixhub_endpoint_);
    }
    shutdown();
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
    if (shutdown_flag_.load()) {
        return;
    }
    if (!server_ || !server_->ok()) {
        return;
    }

    if (server_->wait_for_accept(rix::util::Duration(0.1))) {
        std::weak_ptr<rix::ipc::interfaces::Connection> wconn;
        if (!server_->accept(wconn)) {
            std::cerr << "test1";
            shutdown();
            return;
        } 
        else {
            auto conn = wconn.lock();
            std::cerr << "test2";
            if (conn) {
                rix::msg::mediator::Operation op;
                std::vector<uint8_t> hdr(op.size());
                ssize_t hbytes = conn->read(hdr.data(), hdr.size());
                size_t hoff = 0;
                std::cerr << "test3";
                if (hbytes == static_cast<ssize_t>(hdr.size()) && op.deserialize(hdr.data(), hbytes, hoff)) {
                    if (op.opcode == OPCODE::SUB_NOTIFY && op.len > 0) {
                        std::vector<uint8_t> payload(op.len);
                        ssize_t pbytes = conn->read(payload.data(), payload.size());
                        std::cerr << "test4";
                        if (pbytes == static_cast<ssize_t>(payload.size())) {
                            //bool handled = false;
                            size_t off = 0;
                            rix::msg::mediator::SubNotify notify;
                            std::cerr << "test5";
                            if (!notify.deserialize(payload.data(), payload.size(), off)) {
                                std::cerr << "test6";
                                server_->close(conn);
                                return;
                            } 
                            else {
                                for (const auto &pub : notify.publishers) {
                                    auto c = factory_ ? factory_() : nullptr;
                                    if (!c) {
                                        continue;
                                    }
                                    rix::ipc::Endpoint ep(pub.endpoint.address, pub.endpoint.port);
                                    (void)c->connect(ep);
                                    clients_[pub.id] = c;
                                    std::cerr << "test7";
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    SerializedCallback cb;
    {
        std::lock_guard<std::mutex> g(callback_mutex_);
        cb = callback_;
    }
    if (!cb) {
        return;
    }

    for (auto it = clients_.begin(); it != clients_.end(); ) {
        auto &client = it->second;
        if (!client) {
            it = clients_.erase(it); 
            continue; 
        }

        if (!client->is_connected() || !client->is_readable()) {
            ++it; 
            continue; 
        }

        rix::msg::standard::UInt32 size_prefix;
        std::vector<uint8_t> sbuf(size_prefix.size());
        ssize_t sbytes = client->read(sbuf.data(), sbuf.size());
        size_t soff = 0;
        if (sbytes != static_cast<ssize_t>(sbuf.size()) || !size_prefix.deserialize(sbuf.data(), sbytes, soff)) {
            ++it;
            continue;
        }

        uint32_t msg_size = (static_cast<uint32_t>(sbuf[0]) << 24) |
                    (static_cast<uint32_t>(sbuf[1]) << 16) |
                    (static_cast<uint32_t>(sbuf[2]) << 8)  |
                     static_cast<uint32_t>(sbuf[3]);


        if (msg_size == 0) {
            ++it;
            continue;
        }

        if (!client->is_readable()) {
            ++it; 
            continue; 
        }

        std::vector<uint8_t> mbuf(msg_size);
        ssize_t mbytes = client->read(mbuf.data(), mbuf.size());
        if (mbytes != static_cast<ssize_t>(mbuf.size())) {
            ++it;
            continue;
        }

        cb(mbuf.data(), mbuf.size());
        ++it;
    }
}

}  // namespace core
}  // namespace rix