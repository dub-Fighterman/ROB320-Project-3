#include "rix/core/node.hpp"

namespace rix {
namespace core {

Node::~Node() {
    shutdown();
    
    /**< TODO: Register the node with the mediator */
    if (client_factory_) {
        auto client = client_factory_();
        (void)rix::core::send_message_with_opcode(client, info_, OPCODE::NODE_REGISTER, rixhub_endpoint_);
    }
}

bool Node::ok() const { return !shutdown_flag_; }

void Node::shutdown() { shutdown_flag_ = true; }

void Node::spin_once() {
    // Spin all components, remove ones that are not 'ok'
    auto it = components_.begin();
    while (it != components_.end()) {
        auto component = *it;
        if (!component->ok()) {
            it = components_.erase(it);
            continue;
        }
        component->spin_once();
        it++;
    }
}

std::shared_ptr<Timer> Node::create_timer(const rix::util::Duration &d, Timer::Callback callback) {
    auto timer = std::make_shared<rix::core::Timer>(d, callback);
    components_.push_back(timer);
    return timer;
}

uint64_t Node::generate_id() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    static std::uniform_int_distribution<uint64_t> dis;
    return dis(gen);
}

/**< TODO: Implement the create_publisher method */
std::shared_ptr<Publisher> Node::create_publisher(const rix::msg::mediator::TopicInfo &topic_info,
                                                  const rix::ipc::Endpoint &endpoint) {
    auto server = server_factory_ ? server_factory_(endpoint) : nullptr;
    if (!server || !server->ok()) {
        rix::util::Log::error << "Failed to create publisher server on " << endpoint.to_string() << std::endl;
        return nullptr;
    }

    rix::msg::mediator::PubInfo info;
    info.id = generate_id();
    info.topic_info = topic_info;

    {
        rix::msg::mediator::Endpoint ep_msg;
        ep_msg.address = endpoint.address;
        ep_msg.port    = endpoint.port;
        info.endpoint  = ep_msg;
    }
    
    std::shared_ptr<rix::core::Publisher> pub(new rix::core::Publisher(info, server, client_factory_, rixhub_endpoint_));
    if (!pub) {
        return nullptr;
    }

    {
        //std::lock_guard<std::mutex> guard(components_mutex_);
        components_.push_back(std::static_pointer_cast<rix::core::interfaces::Spinner>(pub));
    }

    return pub;
}

/**< TODO: Implement the create_subscriber method */
std::shared_ptr<Subscriber> Node::create_subscriber(const rix::msg::mediator::TopicInfo &topic_info,
                                                    const rix::ipc::Endpoint &endpoint) {
    auto server = server_factory_ ? server_factory_(endpoint) : nullptr;
    if (!server || !server->ok()) {
        rix::util::Log::error << "Failed to create subscriber server on " << endpoint.to_string() << std::endl;
        return nullptr;
    }

    rix::msg::mediator::SubInfo info;
    info.id = generate_id();
    info.topic_info = topic_info;

    {
        rix::msg::mediator::Endpoint ep_msg;
        ep_msg.address = endpoint.address;
        ep_msg.port    = endpoint.port;
        info.endpoint  = ep_msg;
    }

    std::shared_ptr<rix::core::Subscriber> sub(new rix::core::Subscriber(info, server, client_factory_, rixhub_endpoint_));
    if (!sub) {
        return nullptr;
    }

    {
        //std::lock_guard<std::mutex> guard(components_mutex_);
        components_.push_back(std::static_pointer_cast<rix::core::interfaces::Spinner>(sub));
    }

    return sub;
}

Node::Node(const std::string &name, const rix::ipc::Endpoint &rixhub_endpoint, ServerFactory server_factory,
           ClientFactory client_factory)
    : rixhub_endpoint_(rixhub_endpoint),
      server_factory_(server_factory),
      client_factory_(client_factory),
      shutdown_flag_(false) {
    info_.id = generate_id();
    info_.name = name;

    /**< TODO: Deregister the node with the mediator */
    if (client_factory_) {
        auto client = client_factory_();
        (void)rix::core::send_message_with_opcode_no_response(client, info_, OPCODE::NODE_DEREGISTER, rixhub_endpoint_);
    }
}

}  // namespace core
}  // namespace rix