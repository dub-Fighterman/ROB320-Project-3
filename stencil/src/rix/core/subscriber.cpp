#include "rix/core/subscriber.hpp"

namespace rix {
namespace core {

static bool read_head(const std::shared_ptr<rix::ipc::interfaces::Connection>& c,
                       uint8_t* buffer, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t r = c->read(buffer + off, n - off);
        if (r <= 0) {
            return false;
        }
        off += static_cast<size_t>(r);
    }
    return true;
}

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
    //shutdown();

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

    do{
        if (!server_->wait_for_accept(rix::util::Duration(0.25))) {
            break;
        }
        std::weak_ptr<rix::ipc::interfaces::Connection> wconn;
        if (!server_->accept(wconn)) {
            //std::cerr << "test1";
            break;
        } 
        
        auto conn = wconn.lock();
        //std::cerr << "test2";
        if (!conn) {
            break;
        }

        rix::msg::mediator::Operation op;
        std::vector<uint8_t> hdr(op.size());
        //ssize_t hbytes = conn->read(hdr.data(), hdr.size());
        //size_t hoff = 0;
        //std::cerr << "test3";
        if (!read_head(conn, hdr.data(), hdr.size())) {
            break;
        }
        size_t off = 0;
        if (!op.deserialize(hdr.data(), static_cast<ssize_t>(hdr.size()), off)) {
            break;
        }
        if (op.len > 0) {
            std::vector<uint8_t> payload(op.len);
            if (!read_head(conn, payload.data(), payload.size())) {
                break;
            }
                            
            if (op.opcode != SUB_NOTIFY) {
                break;
            }
            rix::msg::mediator::SubNotify notify;
            off = 0;
            if (!notify.deserialize(payload.data(), static_cast<ssize_t>(payload.size()), off)) {
                break;
            }
            for (const auto &pub : notify.publishers) {
                auto c = factory_ ? factory_() : nullptr;
                if (!c) {
                    continue;
                }
                rix::ipc::Endpoint ep(pub.endpoint.address, pub.endpoint.port);
                (void)c->set_nonblocking(true);
                (void)c->connect(ep);
                 std::lock_guard<std::mutex> g(callback_mutex_);
                clients_[pub.id] = c;
                //std::cerr << "test7";
            }
        }
    }
    while (false);
    

    SerializedCallback cb;
    {
        std::lock_guard<std::mutex> g(callback_mutex_);
        cb = callback_;
    }

    for (auto it = clients_.begin(); it != clients_.end(); ) {
        auto &c = it->second;
        if (!c) {
            it = clients_.erase(it); 
            continue; 
        }

        if (!c->is_connected() || !c->is_readable()) {
            ++it; 
            continue; 
        }

        rix::msg::standard::UInt32 size_prefix;
        std::vector<uint8_t> sbuf(size_prefix.size());
        //ssize_t sbytes = c->read(sbuf.data(), sbuf.size());
        size_t off = 0;
        if (!read_head(c, sbuf.data(), sbuf.size())) {
            ++it;
            continue;
        }

        /**uint32_t msg_size = (static_cast<uint32_t>(sbuf[0]) << 24) |
                    (static_cast<uint32_t>(sbuf[1]) << 16) |
                    (static_cast<uint32_t>(sbuf[2]) << 8)  |
                     static_cast<uint32_t>(sbuf[3]);**/

        if (!size_prefix.deserialize(sbuf.data(), static_cast<ssize_t>(sbuf.size()), off)) {
            ++it;
            continue;
        }

        if (size_prefix.data == 0) {
            ++it;
            continue;
        }

        /**if (!client->is_readable()) {
            ++it; 
            continue; 
        }**/

        std::vector<uint8_t> mbuf(size_prefix.data);
        //ssize_t mbytes = c->read(mbuf.data(), mbuf.size());
        if (!read_head(c, mbuf.data(), mbuf.size())) {
            ++it;
            continue;
        }

        cb(mbuf.data(), mbuf.size());
        ++it;
    }
}

}  // namespace core
}  // namespace rix