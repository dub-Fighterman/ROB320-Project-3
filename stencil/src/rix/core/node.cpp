#include "rix/core/node.hpp"

namespace rix {
namespace core {

Node::~Node() {
    shutdown();
    
    /**< TODO: Register the node with the mediator */
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
    return nullptr;
}

/**< TODO: Implement the create_subscriber method */
std::shared_ptr<Subscriber> Node::create_subscriber(const rix::msg::mediator::TopicInfo &topic_info,
                                                    const rix::ipc::Endpoint &endpoint) {
    return nullptr;
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
}

}  // namespace core
}  // namespace rix