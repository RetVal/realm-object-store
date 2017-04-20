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
#include "util/compiler.hpp"

#include <realm/table_ref.hpp>
#include <realm/data_type.hpp>

#include <functional>
#include <memory>

namespace realm {
class ObjectSchema;
class Query;
class Realm;
class Results;
class SortDescriptor;
class Timestamp;
template<typename> class ThreadSafeReference;

namespace _impl {
class PrimitiveListNotifier;
}

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

    void move(size_t source_ndx, size_t dest_ndx);
    void remove(size_t list_ndx);
    void remove_all();
    void swap(size_t ndx1, size_t ndx2);

    Results sort(bool ascending);
    Results filter(Query q);

    // Return a Results<T> representing a snapshot of this PrimitiveList.
    Results snapshot() const;

    template<typename T>
    T get(size_t row_ndx) const;
    template<typename T>
    T get_unchecked(size_t row_ndx) const noexcept;
    template<typename T>
    size_t find(T value) const;

    template<typename T>
    void add(T value);
    template<typename T>
    void insert(size_t list_ndx, T value);
    template<typename T>
    void set(size_t row_ndx, T value);

    // Get the min/max/average/sum of the values in the list
    // All but sum() returns none when there are zero matching rows
    // sum() returns 0, except for when it returns none
    // Throws UnsupportedColumnTypeException for sum/average on timestamp or non-numeric column
    template<typename T> util::Optional<T> max();
    template<typename T> util::Optional<T> min();
    template<typename T> util::Optional<double> average();
    template<typename T> util::Optional<T> sum();

    template<typename Context>
    auto get(Context&, size_t row_ndx) const;
    template<typename Context>
    auto get_unchecked(Context&, size_t row_ndx) const noexcept;
    template<typename T, typename Context>
    size_t find(Context&, T value) const;

    template<typename T, typename Context>
    void add(Context&, T value);
    template<typename T, typename Context>
    void insert(Context&, size_t list_ndx, T value);
    template<typename T, typename Context>
    void set(Context&, size_t row_ndx, T value);

    // Get the min/max/average/sum of the values in the list
    // All but sum() returns none when there are zero matching rows
    // sum() returns 0, except for when it returns none
    // Throws UnsupportedColumnTypeException for sum/average on timestamp or non-numeric column
    template<typename Context>
    auto max(Context&);
    template<typename Context>
    auto min(Context&);
    template<typename Context>
    auto average(Context&);
    template<typename Context>
    auto sum(Context&);

    bool operator==(PrimitiveList const& rgt) const noexcept;

    NotificationToken add_notification_callback(CollectionChangeCallback cb) &;

private:
    friend ThreadSafeReference<PrimitiveList>;

    std::shared_ptr<Realm> m_realm;
    TableRef m_table;
    _impl::CollectionNotifier::Handle<_impl::PrimitiveListNotifier> m_notifier;

    void verify_valid_row(size_t row_ndx, bool insertion = false) const;

    template<typename Fn>
    auto dispatch(Fn&&) const noexcept;

    int get_type() const noexcept { return 0; }
    bool is_optional() const noexcept { return false; }

    friend struct std::hash<PrimitiveList>;
};

template<typename Fn>
auto PrimitiveList::dispatch(Fn&& fn) const noexcept
{
    switch (get_type()) {
        case type_Int:    return is_optional() ? fn((util::Optional<int64_t>*)0) : fn((int64_t*)0);
        case type_Bool:   return is_optional() ? fn((util::Optional<bool>*)0)    : fn((bool*)0);
        case type_Float:  return is_optional() ? fn((util::Optional<float>*)0)   : fn((float*)0);
        case type_Double: return is_optional() ? fn((util::Optional<double>*)0)  : fn((double*)0);
        case type_String: return fn((StringData*)0);
        case type_Binary: return fn((BinaryData*)0);
        case type_Timestamp: return fn((Timestamp*)0);
        default: REALM_COMPILER_HINT_UNREACHABLE();
    }
}

template<typename Context>
auto PrimitiveList::get(Context& ctx, size_t row_ndx) const
{
    return dispatch([&](auto t) { return ctx.box(get<std::decay_t<decltype(*t)>>(row_ndx)); });
}
template<typename Context>
auto PrimitiveList::get_unchecked(Context& ctx, size_t row_ndx) const noexcept
{
    return dispatch([&](auto t) { return ctx.box(get_unchecked<std::decay_t<decltype(*t)>>(row_ndx)); });
}

template<typename T, typename Context>
size_t PrimitiveList::find(Context& ctx, T value) const
{
    return dispatch([&](auto t) { return find(ctx.template unbox<std::decay_t<decltype(*t)>>(value)); });
}

template<typename T, typename Context>
void PrimitiveList::add(Context& ctx, T value)
{
    dispatch([&](auto t) { add(ctx.template unbox<std::decay_t<decltype(*t)>>(value)); });
}

template<typename T, typename Context>
void PrimitiveList::insert(Context& ctx, size_t list_ndx, T value)
{
    dispatch([&](auto t) { insert(list_ndx, ctx.template unbox<std::decay_t<decltype(*t)>>(value)); });
}

template<typename T, typename Context>
void PrimitiveList::set(Context& ctx, size_t row_ndx, T value)
{
    dispatch([&](auto t) { set(row_ndx, ctx.template unbox<std::decay_t<decltype(*t)>>(value)); });
}

template<typename Context>
auto PrimitiveList::max(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(max<std::decay_t<decltype(*t)>>()); });
}

template<typename Context>
auto PrimitiveList::min(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(min<std::decay_t<decltype(*t)>>()); });
}

template<typename Context>
auto PrimitiveList::average(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(average<std::decay_t<decltype(*t)>>()); });
}

template<typename Context>
auto PrimitiveList::sum(Context& ctx)
{
    return dispatch([&](auto t) { return ctx.box(sum<std::decay_t<decltype(*t)>>()); });
}
} // namespace realm

namespace std {
template<>
struct hash<realm::PrimitiveList> {
    size_t operator()(realm::PrimitiveList const&) const;
};
}

#endif /* REALM_OS_PRIMITIVE_LIST_HPP */
