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

#include "list.hpp"

#include "impl/list_notifier.hpp"
#include "impl/realm_coordinator.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "shared_realm.hpp"
#include "util/format.hpp"

#include <realm/link_view.hpp>

using namespace realm;
using namespace realm::_impl;

List::List() noexcept = default;
List::~List() = default;

List::List(const List&) = default;
List& List::operator=(const List&) = default;
List::List(List&&) = default;
List& List::operator=(List&&) = default;

List::List(std::shared_ptr<Realm> r, LinkViewRef l) noexcept
: m_realm(std::move(r))
, m_link_view(std::move(l))
{
    m_table.reset(&m_link_view->get_target_table());
}

const ObjectSchema& List::get_object_schema() const
{
    verify_attached();

    if (!m_object_schema) {
        auto object_type = ObjectStore::object_type_for_table_name(m_link_view->get_target_table().get_name());
        auto it = m_realm->schema().find(object_type);
        REALM_ASSERT(it != m_realm->schema().end());
        m_object_schema = &*it;
    }
    return *m_object_schema;
}

Query List::get_query() const
{
    verify_attached();
    return m_link_view->get_target_table().where(m_link_view);
}

size_t List::get_origin_row_index() const
{
    verify_attached();
    return m_link_view->get_origin_row_index();
}

void List::verify_valid_row(size_t row_ndx, bool insertion) const
{
    size_t size = m_link_view->size();
    if (row_ndx > size || (!insertion && row_ndx == size)) {
        throw OutOfBoundsIndexException{row_ndx, size + insertion};
    }
}

bool List::is_valid() const
{
    m_realm->verify_thread();
    return m_link_view && m_link_view->is_attached();
}

void List::verify_attached() const
{
    if (!is_valid()) {
        throw InvalidatedException();
    }
}

void List::verify_in_transaction() const
{
    verify_attached();
    if (!m_realm->is_in_transaction()) {
        throw InvalidTransactionException("Must be in a write transaction");
    }
}

size_t List::size() const
{
    verify_attached();
    return m_link_view->size();
}

size_t List::to_table_ndx(size_t row) const noexcept
{
    return m_link_view ? m_link_view->get(row).get_index() : row;
}

namespace {
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

template<>
auto get<RowExpr, void>(Table& table, size_t row)
{
    return table.get(row);
}
}

template<typename T>
T List::get(size_t row_ndx) const
{
    verify_attached();
    verify_valid_row(row_ndx);
    return ::get<T>(*m_table, to_table_ndx(row_ndx));
}

template RowExpr List::get(size_t) const;

size_t List::get_unchecked(size_t row_ndx) const noexcept
{
    return m_link_view->get(row_ndx).get_index();
}

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
bool is_null(T) { return false; }
template<typename T>
bool is_null(util::Optional<T> value) { return !value; }

template<typename T>
size_t List::find(T const& value) const
{
    verify_attached();
    return is_null(value) ? m_table->find_first_null(0) : m_table->find_first(0, unbox(value));
}

template<>
size_t List::find(ConstRow const& row) const
{
    verify_attached();

    if (!row.is_attached() || row.get_table() != &m_link_view->get_target_table()) {
        return not_found;
    }

    return m_link_view->find(row.get_index());
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
void List::add(T value)
{
    verify_in_transaction();
    ::set(*m_table, m_table->add_empty_row(), value);
}

template<>
void List::add(size_t target_row_ndx)
{
    verify_in_transaction();
    m_link_view->add(target_row_ndx);
}

template<>
void List::add(int value)
{
    verify_in_transaction();
    if (m_link_view)
        add(static_cast<size_t>(value));
    else
        add(static_cast<int64_t>(value));
}

template<typename T>
void List::insert(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx, true);
    m_table->insert_empty_row(row_ndx);
    ::set(*m_table, row_ndx, value);
}

template<>
void List::insert(size_t row_ndx, size_t target_row_ndx)
{
    verify_in_transaction();
    verify_valid_row(row_ndx, true);
    m_link_view->insert(row_ndx, target_row_ndx);
}

void List::move(size_t source_ndx, size_t dest_ndx)
{
    verify_in_transaction();
    verify_valid_row(source_ndx);
    verify_valid_row(dest_ndx); // Can't be one past end due to removing one earlier
    if (m_link_view)
        m_link_view->move(source_ndx, dest_ndx);
    else
        throw std::logic_error("not supported");
}

void List::remove(size_t row_ndx)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    if (m_link_view)
        m_link_view->remove(row_ndx);
    else
        m_table->remove(row_ndx);
}

void List::remove_all()
{
    verify_in_transaction();
    if (m_link_view)
        m_link_view->clear();
    else
        m_table->clear();
}

template<typename T>
void List::set(size_t row_ndx, T value)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    ::set(*m_table, row_ndx, value);
}

template<>
void List::set(size_t row_ndx, size_t target_row_ndx)
{
    verify_in_transaction();
    verify_valid_row(row_ndx);
    m_link_view->set(row_ndx, target_row_ndx);
}

void List::swap(size_t ndx1, size_t ndx2)
{
    verify_in_transaction();
    verify_valid_row(ndx1);
    verify_valid_row(ndx2);
    if (m_link_view)
        m_link_view->swap(ndx1, ndx2);
    else
        m_table->swap_rows(ndx1, ndx2);
}

void List::delete_all()
{
    verify_in_transaction();
    if (m_link_view)
        m_link_view->remove_all_target_rows();
    else
        m_table->clear();
}

Results List::sort(SortDescriptor order)
{
    verify_attached();
    if (m_link_view)
        return Results(m_realm, m_link_view, util::none, std::move(order));
    return Results(m_realm, get_query(), std::move(order));
}

Results List::filter(Query q)
{
    verify_attached();
    if (m_link_view)
        return Results(m_realm, m_link_view, get_query().and_query(std::move(q)));
    return Results(m_realm, get_query().and_query(std::move(q)));
}

Results List::snapshot() const
{
    verify_attached();
    return Results(m_realm, m_link_view).snapshot();
}

template<typename T>
util::Optional<T> List::max(size_t col)
{
    return Results(m_realm, *m_table).max<T>(col);
}

template<typename T>
util::Optional<T> List::min(size_t col)
{
    return Results(m_realm, *m_table).min<T>(col);
}

template<typename T>
util::Optional<T> List::sum(size_t col)
{
    return Results(m_realm, *m_table).sum<T>(col);
}

template<typename T>
util::Optional<T> List::average(size_t col)
{
    // FIXME
    return Results(m_realm, *m_table).average<T>(col);
}

template<>
util::Optional<Mixed> List::max(size_t column)
{
    return Results(m_realm, m_link_view).max(column);
}

template<>
util::Optional<Mixed> List::min(size_t column)
{
    return Results(m_realm, m_link_view).min(column);
}

template<>
util::Optional<Mixed> List::sum(size_t column)
{
    return Results(m_realm, m_link_view).sum(column);
}

template<>
util::Optional<Mixed> List::average(size_t column)
{
    return Results(m_realm, m_link_view).average(column);
}

// These definitions rely on that LinkViews are interned by core
bool List::operator==(List const& rgt) const noexcept
{
    // FIXME subtable
    return m_link_view.get() == rgt.m_link_view.get();
}

namespace std {
size_t hash<realm::List>::operator()(realm::List const& list) const
{
    // FIXME subtable
    return std::hash<void*>()(list.m_link_view.get());
}
}

NotificationToken List::add_notification_callback(CollectionChangeCallback cb) &
{
    verify_attached();
    if (!m_notifier) {
        m_notifier = std::make_shared<ListNotifier>(m_link_view, m_realm);
        RealmCoordinator::register_notifier(m_notifier);
    }
    return {m_notifier, m_notifier->add_callback(std::move(cb))};
}

List::OutOfBoundsIndexException::OutOfBoundsIndexException(size_t r, size_t c)
: std::out_of_range(util::format("Requested index %1 greater than max %2", r, c))
, requested(r), valid_count(c) {}

namespace realm {
#define REALM_PRIMITIVE_LIST_TYPE(T) \
    template T List::get<T>(size_t) const; \
    template size_t List::find<T>(T const&) const; \
    template void List::add<T>(T); \
    template void List::insert<T>(size_t, T); \
    template void List::set<T>(size_t, T); \
    template util::Optional<T> List::max<T>(size_t); \
    template util::Optional<T> List::min<T>(size_t); \
    template util::Optional<T> List::average<T>(size_t); \
    template util::Optional<T> List::sum<T>(size_t);

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
