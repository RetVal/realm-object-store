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
#include "primitive_results.hpp"
#include "schema.hpp"
#include "shared_realm.hpp"
#include "util/format.hpp"

#include <realm/table.hpp>

using namespace realm;
using namespace realm::_impl;

template<typename T>
PrimitiveList<T>::PrimitiveList() noexcept = default;
template<typename T>
PrimitiveList<T>::~PrimitiveList() = default;

template<typename T>
PrimitiveList<T>::PrimitiveList(const PrimitiveList&) = default;
template<typename T>
PrimitiveList<T>& PrimitiveList<T>::operator=(const PrimitiveList&) = default;
template<typename T>
PrimitiveList<T>::PrimitiveList(PrimitiveList&&) = default;
template<typename T>
PrimitiveList<T>& PrimitiveList<T>::operator=(PrimitiveList&&) = default;

template<typename T>
PrimitiveList<T>::PrimitiveList(std::shared_ptr<Realm> r, TableRef t) noexcept
: m_realm(std::move(r))
, m_table(std::move(t))
{
}

template<typename T>
Query PrimitiveList<T>::get_query() const
{
    verify_attached();
    return m_table->where();
}

template<typename T>
size_t PrimitiveList<T>::get_origin_row_index() const
{
    verify_attached();
    return m_table->get_parent_row_index();
}

template<typename T>
void PrimitiveList<T>::verify_valid_row(size_t row_ndx, bool insertion) const
{
    size_t size = m_table->size();
    if (row_ndx > size || (!insertion && row_ndx == size)) {
        throw List::OutOfBoundsIndexException{row_ndx, size + insertion};
    }
}

template<typename T>
bool PrimitiveList<T>::is_valid() const
{
    m_realm->verify_thread();
    return m_table && m_table->is_attached();
}

template<typename T>
void PrimitiveList<T>::verify_attached() const
{
    if (!is_valid()) {
        throw List::InvalidatedException();
    }
}

template<typename T>
void PrimitiveList<T>::verify_in_transaction() const
{
    verify_attached();
    if (!m_realm->is_in_transaction()) {
        throw InvalidTransactionException("Must be in a write transaction");
    }
}

template<typename T>
size_t PrimitiveList<T>::size() const
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
T PrimitiveList<T>::get(size_t row_ndx) const
{
    verify_attached();
    verify_valid_row(row_ndx);
    return ::get<T>(*m_table, row_ndx);
}

template<typename T>
T PrimitiveList<T>::get_unchecked(size_t row_ndx) const noexcept
{
    return ::get<T>(*m_table, row_ndx);
}

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
bool is_null(T) { return false; }
template<typename T>
bool is_null(util::Optional<T> value) { return !value; }

template<typename T>
size_t PrimitiveList<T>::find(T value) const
{
    verify_attached();
    return is_null(value) ? m_table->find_first_null(0) : m_table->find_first(0, unbox(value));
}

template<typename T>
void PrimitiveList<T>::add(T value)
{
    verify_in_transaction();
    m_table->set(0, m_table->add_empty_row(), value);
}

template<typename T>
void PrimitiveList<T>::insert(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx, true);
    m_table->insert_empty_row(row_ndx);
    m_table->set(0, row_ndx, value);
}

template<typename T>
void PrimitiveList<T>::move(size_t source_ndx, size_t dest_ndx)
{
    verify_in_transaction();
    verify_valid_row(source_ndx);
    verify_valid_row(dest_ndx); // Can't be one past end due to removing one earlier
//    m_table->move(source_ndx, dest_ndx);
}

template<typename T>
void PrimitiveList<T>::remove(size_t row_ndx)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    m_table->remove(row_ndx);
}

template<typename T>
void PrimitiveList<T>::remove_all()
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
void PrimitiveList<T>::set(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    ::set(*m_table, row_ndx, value);
}

template<typename T>
void PrimitiveList<T>::swap(size_t ndx1, size_t ndx2)
{
    verify_in_transaction();
    verify_valid_row(ndx1);
    verify_valid_row(ndx2);
    m_table->swap_rows(ndx1, ndx2);
}

template<typename T>
PrimitiveResults<T> PrimitiveList<T>::sort(bool ascending)
{
    verify_attached();
    return PrimitiveResults<T>(m_realm, get_query(),
                               ascending ? PrimitiveResults<T>::Sort::Ascending : PrimitiveResults<T>::Sort::Descending);
}

template<typename T>
PrimitiveResults<T> PrimitiveList<T>::filter(Query q)
{
    verify_attached();
    return PrimitiveResults<T>(m_realm, get_query().and_query(std::move(q)));
}

template<typename T>
PrimitiveResults<T> PrimitiveList<T>::snapshot() const
{
    verify_attached();
    return PrimitiveResults<T>(m_realm, *m_table).snapshot();
}

template<typename T>
util::Optional<T> PrimitiveList<T>::max()
{
    return PrimitiveResults<T>(m_realm, *m_table).max();
}

template<typename T>
util::Optional<T> PrimitiveList<T>::min()
{
    return PrimitiveResults<T>(m_realm, *m_table).min();
}

template<typename T>
util::Optional<T> PrimitiveList<T>::sum()
{
    return PrimitiveResults<T>(m_realm, *m_table).sum();
}

template<typename T>
util::Optional<double> PrimitiveList<T>::average()
{
    return PrimitiveResults<T>(m_realm, *m_table).average();
}

// These definitions rely on that LinkViews are interned by core
template<typename T>
bool PrimitiveList<T>::operator==(PrimitiveList const& rgt) const noexcept
{
    return m_table.get() == rgt.m_table.get();
}

namespace std {
template<typename T>
size_t hash<realm::PrimitiveList<T>>::operator()(realm::PrimitiveList<T> const& list) const
{
    return std::hash<void*>()(list.m_table.get());
}
}

template<typename T>
NotificationToken PrimitiveList<T>::add_notification_callback(CollectionChangeCallback cb) &
{
    verify_attached();
    if (!m_notifier) {
        m_notifier = std::make_shared<PrimitiveListNotifier>(m_table, m_realm);
        RealmCoordinator::register_notifier(m_notifier);
    }
    return {m_notifier, m_notifier->add_callback(std::move(cb))};
}

namespace realm {
template class PrimitiveList<bool>;
template class PrimitiveList<int64_t>;
template class PrimitiveList<float>;
template class PrimitiveList<double>;
template class PrimitiveList<StringData>;
template class PrimitiveList<BinaryData>;
template class PrimitiveList<Timestamp>;
template class PrimitiveList<util::Optional<bool>>;
template class PrimitiveList<util::Optional<int64_t>>;
template class PrimitiveList<util::Optional<float>>;
template class PrimitiveList<util::Optional<double>>;
}
