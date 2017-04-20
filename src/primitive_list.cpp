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

#include "primitive_list.hpp"

#include "impl/primitive_list_notifier.hpp"
#include "impl/realm_coordinator.hpp"
#include "list.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "shared_realm.hpp"
#include "util/format.hpp"

#include <realm/table.hpp>

using namespace realm;
using namespace realm::_impl;

PrimitiveList::PrimitiveList() noexcept = default;
PrimitiveList::~PrimitiveList() = default;

PrimitiveList::PrimitiveList(const PrimitiveList&) = default;
PrimitiveList& PrimitiveList::operator=(const PrimitiveList&) = default;
PrimitiveList::PrimitiveList(PrimitiveList&&) = default;
PrimitiveList& PrimitiveList::operator=(PrimitiveList&&) = default;

PrimitiveList::PrimitiveList(std::shared_ptr<Realm> r, TableRef t) noexcept
: m_realm(std::move(r))
, m_table(std::move(t))
{
}

Query PrimitiveList::get_query() const
{
    verify_attached();
    return m_table->where();
}

size_t PrimitiveList::get_origin_row_index() const
{
    verify_attached();
    return m_table->get_parent_row_index();
}

void PrimitiveList::verify_valid_row(size_t row_ndx, bool insertion) const
{
    size_t size = m_table->size();
    if (row_ndx > size || (!insertion && row_ndx == size)) {
        throw List::OutOfBoundsIndexException{row_ndx, size + insertion};
    }
}

bool PrimitiveList::is_valid() const
{
    m_realm->verify_thread();
    return m_table && m_table->is_attached();
}

void PrimitiveList::verify_attached() const
{
    if (!is_valid()) {
        throw List::InvalidatedException();
    }
}

void PrimitiveList::verify_in_transaction() const
{
    verify_attached();
    if (!m_realm->is_in_transaction()) {
        throw InvalidTransactionException("Must be in a write transaction");
    }
}

size_t PrimitiveList::size() const
{
    verify_attached();
    return m_table->size();
}

template<typename T>
constexpr auto is_dereferencable(int) -> decltype(*T(), bool()) { return true; }
template<typename T>
constexpr bool is_dereferencable(...) { return false; }

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
T unbox(T value) { return value; }
template<typename T>
T unbox(util::Optional<T> value) { return *value; }

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
auto get(Table& table, size_t row)
{
    return table.get<T>(0, row);
}

template<typename T, typename = void, typename = std::enable_if_t<is_dereferencable<T>(0)>>
T get(Table& table, size_t row)
{
    if (table.is_null(0, row))
        return util::none;
    return table.get<decltype(unbox(T()))>(0, row);
}

template<typename T>
T PrimitiveList::get(size_t row_ndx) const
{
    verify_attached();
    verify_valid_row(row_ndx);
    return ::get<T>(*m_table, row_ndx);
}

template<typename T>
T PrimitiveList::get_unchecked(size_t row_ndx) const noexcept
{
    return ::get<T>(*m_table, row_ndx);
}

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
bool is_null(T) { return false; }
template<typename T>
bool is_null(util::Optional<T> value) { return !value; }

template<typename T>
size_t PrimitiveList::find(T value) const
{
    verify_attached();
    return is_null(value) ? m_table->find_first_null(0) : m_table->find_first(0, unbox(value));
}

template<typename T>
void PrimitiveList::add(T value)
{
    verify_in_transaction();
    m_table->set(0, m_table->add_empty_row(), value);
}

template<typename T>
void PrimitiveList::insert(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx, true);
    m_table->insert_empty_row(row_ndx);
    m_table->set(0, row_ndx, value);
}

void PrimitiveList::move(size_t source_ndx, size_t dest_ndx)
{
    verify_in_transaction();
    verify_valid_row(source_ndx);
    verify_valid_row(dest_ndx); // Can't be one past end due to removing one earlier
//    m_table->move(source_ndx, dest_ndx);
}

void PrimitiveList::remove(size_t row_ndx)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    m_table->remove(row_ndx);
}

void PrimitiveList::remove_all()
{
    verify_in_transaction();
    m_table->clear();
}

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
void set(Table& table, size_t row, T value)
{
    table.set(0, row, value);
}

template<typename T>
void set(Table& table, size_t row, util::Optional<T> value)
{
    if (value)
        table.set(0, row, *value);
    else
        table.set_null(0, row);
}

template<typename T>
void PrimitiveList::set(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    ::set(*m_table, row_ndx, value);
}

void PrimitiveList::swap(size_t ndx1, size_t ndx2)
{
    verify_in_transaction();
    verify_valid_row(ndx1);
    verify_valid_row(ndx2);
    m_table->swap_rows(ndx1, ndx2);
}

Results PrimitiveList::sort(bool ascending)
{
    verify_attached();
    return Results();
    // FIXME
//    return Results(m_realm, get_query(),
//                            ascending ? Results::Sort::Ascending
//                                      : Results::Sort::Descending);
}

Results PrimitiveList::filter(Query q)
{
    verify_attached();
    return Results(m_realm, get_query().and_query(std::move(q)));
}

Results PrimitiveList::snapshot() const
{
    verify_attached();
    return Results(m_realm, *m_table).snapshot();
}

template<typename T>
util::Optional<T> PrimitiveList::max()
{
    return Results(m_realm, *m_table).max<T>(0);
}

template<typename T>
util::Optional<T> PrimitiveList::min()
{
    return Results(m_realm, *m_table).min<T>(0);
}

template<typename T>
util::Optional<T> PrimitiveList::sum()
{
    return Results(m_realm, *m_table).sum<T>(0);
}

template<typename T>
util::Optional<double> PrimitiveList::average()
{
    return Results(m_realm, *m_table).average<double>(0);
}

// These definitions rely on that LinkViews are interned by core
bool PrimitiveList::operator==(PrimitiveList const& rgt) const noexcept
{
    return m_table.get() == rgt.m_table.get();
}

// namespace std {
// template<>
// size_t hash<realm::PrimitiveList>::operator()(realm::PrimitiveList const& list) const
// {
//     return std::hash<void*>()(list.m_table.get());
// }
// }

NotificationToken PrimitiveList::add_notification_callback(CollectionChangeCallback cb) &
{
    verify_attached();
    if (!m_notifier) {
        m_notifier = std::make_shared<PrimitiveListNotifier>(m_table, m_realm);
        RealmCoordinator::register_notifier(m_notifier);
    }
    return {m_notifier, m_notifier->add_callback(std::move(cb))};
}

namespace realm {
#define REALM_PRIMITIVE_LIST_TYPE(T) \
    template T PrimitiveList::get<T>(size_t) const; \
    template T PrimitiveList::get_unchecked<T>(size_t) const noexcept; \
    template size_t PrimitiveList::find<T>(T) const; \
    template void PrimitiveList::add<T>(T); \
    template void PrimitiveList::insert<T>(size_t, T); \
    template void PrimitiveList::set<T>(size_t, T); \
    template util::Optional<T> PrimitiveList::max<T>(); \
    template util::Optional<T> PrimitiveList::min<T>(); \
    template util::Optional<double> PrimitiveList::average<T>(); \
    template util::Optional<T> PrimitiveList::sum<T>();

REALM_PRIMITIVE_LIST_TYPE(bool)
REALM_PRIMITIVE_LIST_TYPE(int64_t)
REALM_PRIMITIVE_LIST_TYPE(float)
REALM_PRIMITIVE_LIST_TYPE(double)
REALM_PRIMITIVE_LIST_TYPE(StringData)
REALM_PRIMITIVE_LIST_TYPE(BinaryData)
REALM_PRIMITIVE_LIST_TYPE(Timestamp)
REALM_PRIMITIVE_LIST_TYPE(util::Optional<bool>)
REALM_PRIMITIVE_LIST_TYPE(util::Optional<int64_t>)
REALM_PRIMITIVE_LIST_TYPE(util::Optional<float>)
REALM_PRIMITIVE_LIST_TYPE(util::Optional<double>)

#undef REALM_PRIMITIVE_LIST_TYPE
}
