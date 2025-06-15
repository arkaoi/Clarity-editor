#ifndef SKIPLIST_HPP_
#define SKIPLIST_HPP_

#include <cassert>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <random>
#include <utility>
#include <vector>

template <
    typename Key,
    typename Value,
    typename Compare = std::less<Key>,
    int MAX_LEVEL = 16>
class SkipListMap {
private:
    static constexpr double probability = 0.5;

    struct Node {
        Key key;
        Value value;
        std::vector<Node *> forward;

        Node(int lvl, const Key &k, const Value &v)
            : key(k), value(v), forward(lvl + 1, nullptr) {
        }
    };

    Node *head_;
    int level_;
    size_t size_;
    Compare cmp_;
    std::mt19937_64 rnd_;

    int randomLevel() {
        int lvl = 0;
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        while (lvl < MAX_LEVEL && dist(rnd_) < probability) {
            ++lvl;
        }
        return lvl;
    }

public:
    SkipListMap()
        : head_(new Node(MAX_LEVEL, Key(), Value())),
          level_(0),
          size_(0),
          rnd_(std::random_device{}()) {
    }

    ~SkipListMap() {
        Node *cur = head_->forward[0];
        while (cur) {
            Node *next = cur->forward[0];
            delete cur;
            cur = next;
        }
        delete head_;
    }

    size_t size() const noexcept {
        return size_;
    }

    bool empty() const noexcept {
        return size_ == 0;
    }

    void clear() noexcept {
        Node *cur = head_->forward[0];
        while (cur) {
            Node *next = cur->forward[0];
            delete cur;
            cur = next;
        }
        for (int i = 0; i <= level_; ++i) {
            head_->forward[i] = nullptr;
        }
        level_ = 0;
        size_ = 0;
    }

    void insert(const Key &key, const Value &val) {
        std::vector<Node *> update(MAX_LEVEL + 1);
        Node *x = head_;
        for (int i = level_; i >= 0; --i) {
            while (x->forward[i] && cmp_(x->forward[i]->key, key)) {
                x = x->forward[i];
            }
            update[i] = x;
        }
        x = x->forward[0];
        if (x && !cmp_(key, x->key) && !cmp_(x->key, key)) {
            x->value = val;
        } else {
            int lvl = randomLevel();
            if (lvl > level_) {
                for (int i = level_ + 1; i <= lvl; ++i) {
                    update[i] = head_;
                }
                level_ = lvl;
            }
            Node *newNode = new Node(lvl, key, val);
            for (int i = 0; i <= lvl; ++i) {
                newNode->forward[i] = update[i]->forward[i];
                update[i]->forward[i] = newNode;
            }
            ++size_;
        }
    }

    bool erase(const Key &key) {
        std::vector<Node *> update(MAX_LEVEL + 1);
        Node *x = head_;
        for (int i = level_; i >= 0; --i) {
            while (x->forward[i] && cmp_(x->forward[i]->key, key)) {
                x = x->forward[i];
            }
            update[i] = x;
        }
        x = x->forward[0];
        if (!x || cmp_(key, x->key) || cmp_(x->key, key)) {
            return false;
        }
        for (int i = 0; i <= level_; ++i) {
            if (update[i]->forward[i] != x) {
                break;
            }
            update[i]->forward[i] = x->forward[i];
        }
        delete x;
        while (level_ > 0 && !head_->forward[level_]) {
            --level_;
        }
        --size_;
        return true;
    }

    Value *find(const Key &key) {
        Node *x = head_;
        for (int i = level_; i >= 0; --i) {
            while (x->forward[i] && cmp_(x->forward[i]->key, key)) {
                x = x->forward[i];
            }
        }
        x = x->forward[0];
        if (x && !cmp_(key, x->key) && !cmp_(x->key, key)) {
            return &x->value;
        }
        return nullptr;
    }

    Value &operator[](const Key &key) {
        if (auto *v = find(key)) {
            return *v;
        }
        insert(key, Value());
        return *find(key);
    }

    class iterator {
        Node *n_;

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<const Key, Value>;
        using reference = value_type;

        explicit iterator(Node *p) : n_(p) {
        }

        iterator &operator++() {
            n_ = n_->forward[0];
            return *this;
        }

        reference operator*() const {
            return {n_->key, n_->value};
        }

        bool operator==(iterator o) const {
            return n_ == o.n_;
        }

        bool operator!=(iterator o) const {
            return !(*this == o);
        }
    };

    iterator begin() {
        return iterator(head_->forward[0]);
    }

    iterator end() {
        return iterator(nullptr);
    }

    iterator begin() const {
        return const_cast<SkipListMap *>(this)->begin();
    }

    iterator end() const {
        return const_cast<SkipListMap *>(this)->end();
    }
};

#endif  // SKIPLIST_HPP_