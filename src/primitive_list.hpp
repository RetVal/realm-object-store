////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_OS_PRIMITIVE_LIST_HPP
#define REALM_OS_PRIMITIVE_LIST_HPP

#include "collection_notifications.hpp"
#include "impl/collection_notifier.hpp"

#include <realm/table_ref.hpp>

#include <functional>
#include <memory>

namespace realm {
class ObjectSchema;
class Query;
class Realm;
class SortDescriptor;
template<typename> class PrimitiveResults;
template<typename> class ThreadSafeReference;

namespace _impl {
class PrimitiveListNotifier;
}

template<typename T>
class PrimitiveList {
public:
    PrimitiveList() noexcept;
    PrimitiveList(std::shared_ptr<Realm> r, TableRef t) noexcept;
    ~PrimitiveList();

    PrimitiveList(const PrimitiveList&);
    PrimitiveList& operator=(const PrimitiveList&);
    PrimitiveList(PrimitiveList&&);
    PrimitiveList& operator=(PrimitiveList&&);

    std::shared_ptr<Realm> const& get_realm() const { return m_realm; }
    Query get_query() const;
    size_t get_origin_row_index() const;

    bool is_valid() const;
    void verify_attached() const;
    void verify_in_transaction() const;

    size_t size() const;
    T get(size_t row_ndx) const;
    T get_unchecked(size_t row_ndx) const noexcept;
    size_t find(T value) const;

    void add(T value);
    void insert(size_t list_ndx, T value);
    void move(size_t source_ndx, size_t dest_ndx);
    void remove(size_t list_ndx);
    void remove_all();
    void set(size_t row_ndx, T value);
    void swap(size_t ndx1, size_t ndx2);

    PrimitiveResults<T> sort(bool ascending);
    PrimitiveResults<T> filter(Query q);

    // Return a PrimitiveResults<T> representing a snapshot of this PrimitiveList.
    PrimitiveResults<T> snapshot() const;

    // Get the min/max/average/sum of the values in the list
    // All but sum() returns none when there are zero matching rows
    // sum() returns 0, except for when it returns none
    // Throws UnsupportedColumnTypeException for sum/average on timestamp or non-numeric column
    util::Optional<T> max();
    util::Optional<T> min();
    util::Optional<double> average();
    util::Optional<T> sum();

    bool operator==(PrimitiveList const& rgt) const noexcept;

    NotificationToken add_notification_callback(CollectionChangeCallback cb) &;

private:
    friend ThreadSafeReference<PrimitiveList>;

    std::shared_ptr<Realm> m_realm;
    TableRef m_table;
    _impl::CollectionNotifier::Handle<_impl::PrimitiveListNotifier> m_notifier;

    void verify_valid_row(size_t row_ndx, bool insertion = false) const;

    friend struct std::hash<PrimitiveList>;
};
} // namespace realm

namespace std {
template<typename T>
struct hash<realm::PrimitiveList<T>> {
    size_t operator()(realm::PrimitiveList<T> const&) const;
};
}

#endif /* REALM_OS_PRIMITIVE_LIST_HPP */
