////////////////////////////////////////////////////////////////////////////
//
// Copyright 2017 Realm Inc.
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

#include "primitive_results.hpp"

#include "impl/realm_coordinator.hpp"
#include "impl/results_notifier.hpp"
#include "object_schema.hpp"
#include "object_store.hpp"
#include "results.hpp"
#include "schema.hpp"
#include "util/compiler.hpp"
#include "util/format.hpp"

#include <stdexcept>

using namespace realm;

template<typename T>
PrimitiveResults<T>::PrimitiveResults() = default;
template<typename T>
PrimitiveResults<T>::~PrimitiveResults() = default;

template<typename T>
PrimitiveResults<T>::PrimitiveResults(std::shared_ptr<Realm> r, Query q, Sort s, bool d)
: _impl::ResultsBase(std::move(r), std::move(q))
, m_sort(s)
{
}

template<typename T>
PrimitiveResults<T>::PrimitiveResults(std::shared_ptr<Realm> r, Table& table)
: _impl::ResultsBase(std::move(r), table)
{
}

template<typename T>
PrimitiveResults<T>::PrimitiveResults(std::shared_ptr<Realm> r, TableView tv, Sort s, bool d)
: _impl::ResultsBase(std::move(r), std::move(tv))
, m_sort(s)
{
}

template<typename T>
bool PrimitiveResults<T>::is_distinct() const noexcept
{
    return (bool)ResultsBase::get_distinct();
}

template<typename T>
PrimitiveResults<T>::PrimitiveResults(const PrimitiveResults&) = default;
template<typename T>
PrimitiveResults<T>& PrimitiveResults<T>::operator=(const PrimitiveResults&) = default;
template<typename T>
PrimitiveResults<T>::PrimitiveResults(PrimitiveResults&& other) = default;
template<typename T>
PrimitiveResults<T>& PrimitiveResults<T>::operator=(PrimitiveResults&& other) = default;

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
T PrimitiveResults<T>::get(size_t row_ndx)
{
    validate_read();
    switch (get_mode()) {
        case Mode::Empty: break;
        case Mode::Table:
            if (row_ndx < table()->size())
                return ::get<T>(*table(), row_ndx);
            break;
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            if (row_ndx >= tableview().size())
                break;
            if (!auto_update() && !tableview().is_row_attached(row_ndx))
                return {};
            return ::get<T>(*table(), tableview().get(row_ndx).get_index());
        default: REALM_COMPILER_HINT_UNREACHABLE();
    }

    throw Results::OutOfBoundsIndexException{row_ndx, size()};
}

template<typename T>
util::Optional<T> PrimitiveResults<T>::first()
{
    validate_read();
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return table()->size() == 0 ? util::none : make_optional(::get<T>(*table(), 0));
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            if (tableview().size() == 0)
                return util::none;
            else if (!auto_update() && !tableview().is_row_attached(0))
                return none;
            return ::get<T>(*table(), tableview().get(0).get_index());
    }
}

template<typename T>
util::Optional<T> PrimitiveResults<T>::last()
{
    validate_read();
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return table()->size() == 0 ? util::none : make_optional(::get<T>(*table(), table()->size() - 1));
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            auto s = tableview().size();
            if (s == 0)
                return util::none;
            else if (!auto_update() && !tableview().is_row_attached(s - 1))
                return none;
            return ::get<T>(*table(), tableview().back().get_index());
    }
}

template<typename T, typename = std::enable_if_t<!is_dereferencable<T>(0)>>
bool is_null(T) { return false; }
template<typename T>
bool is_null(util::Optional<T> value) { return !value; }

template<typename T> size_t find_first(Table& table, T value)
{
    return is_null(value) ? table.find_first_null(0) : table.find_first(0, unbox(value));
}
template<typename T> size_t find_first(TableView& table, T value)
{
    return is_null(value) ? table.get_parent().where(&table).equal(0, realm::null()).find()
                          : table.find_first(0, unbox(value));
}
template<> size_t find_first(TableView& table, bool value)
{
    return table.find_first<int64_t>(0, value);
}
template<> size_t find_first(TableView& table, util::Optional<bool> value)
{
    return is_null(value) ? table.get_parent().where(&table).equal(0, realm::null()).find()
                          : table.find_first<int64_t>(0, unbox(value));
}

#define REALM_FIND_FIRST(Type, type_name) \
template<> size_t find_first<Type>(Table& table, Type value) \
{ \
    return table.find_first_##type_name(0, value); \
} \
template<> size_t find_first<Type>(TableView& table, Type value) \
{ \
    return table.find_first_##type_name(0, value); \
}
REALM_FIND_FIRST(int64_t, int)
REALM_FIND_FIRST(float, float)
REALM_FIND_FIRST(double, double)
REALM_FIND_FIRST(Timestamp, timestamp)
REALM_FIND_FIRST(StringData, string)
REALM_FIND_FIRST(BinaryData, binary)

template<typename T>
size_t PrimitiveResults<T>::index_of(T value)
{
    validate_read();
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return not_found;
        case Mode::Table:
            return ::find_first(*table(), value);
        case Mode::Query:
        case Mode::TableView:
            update_tableview();
            return ::find_first(tableview(), value);
    }
}

#define REALM_DECLARE_AGGREGATE_FUNCTIONS(name) \
    template<typename T, typename U> util::Optional<T> name(Table&) { throw "unsupported"; } \
    template<typename T, typename U> util::Optional<T> name(TableView&) { throw "unsupported"; }
REALM_DECLARE_AGGREGATE_FUNCTIONS(maximum)
REALM_DECLARE_AGGREGATE_FUNCTIONS(minimum)
REALM_DECLARE_AGGREGATE_FUNCTIONS(average)
REALM_DECLARE_AGGREGATE_FUNCTIONS(sum)
#undef REALM_DECLARE_AGGREGATE_FUNCTIONS

#define REALM_MINMAX_FUNCTION(TableType, name, Type, type_name) \
template<> \
util::Optional<Type> name<Type, Type>(TableType& table) \
{ \
    size_t ndx = 0; \
    auto value = table.name##_##type_name(0, &ndx); \
    return ndx == npos ? none : util::make_optional(value); \
}

REALM_MINMAX_FUNCTION(Table, maximum, int64_t, int)
REALM_MINMAX_FUNCTION(TableView, maximum, int64_t, int)
REALM_MINMAX_FUNCTION(Table, maximum, float, float)
REALM_MINMAX_FUNCTION(TableView, maximum, float, float)
REALM_MINMAX_FUNCTION(Table, maximum, double, double)
REALM_MINMAX_FUNCTION(TableView, maximum, double, double)
REALM_MINMAX_FUNCTION(Table, maximum, Timestamp, timestamp)
REALM_MINMAX_FUNCTION(TableView, maximum, Timestamp, timestamp)

REALM_MINMAX_FUNCTION(Table, minimum, int64_t, int)
REALM_MINMAX_FUNCTION(TableView, minimum, int64_t, int)
REALM_MINMAX_FUNCTION(Table, minimum, float, float)
REALM_MINMAX_FUNCTION(TableView, minimum, float, float)
REALM_MINMAX_FUNCTION(Table, minimum, double, double)
REALM_MINMAX_FUNCTION(TableView, minimum, double, double)
REALM_MINMAX_FUNCTION(Table, minimum, Timestamp, timestamp)
REALM_MINMAX_FUNCTION(TableView, minimum, Timestamp, timestamp)

#undef REALM_MINMAX_FUNCTION

#define REALM_SUM_FUNCTION(TableType, Type, type_name) \
template<> \
util::Optional<Type> sum<Type, Type>(TableType& table) \
{ \
    return table.sum_##type_name(0); \
}

REALM_SUM_FUNCTION(Table, int64_t, int)
REALM_SUM_FUNCTION(TableView, int64_t, int)
REALM_SUM_FUNCTION(Table, float, float)
REALM_SUM_FUNCTION(TableView, double, double)

#undef REALM_SUM_FUNCTION

#define REALM_AVERAGE_FUNCTION(TableType, Type, type_name) \
template<> \
util::Optional<double> average<double, Type>(TableType& table) \
{ \
    size_t non_nil_count = 0; \
    auto value = table.average_##type_name(0, &non_nil_count); \
    return non_nil_count == 0 ? none : make_optional(value); \
}

REALM_AVERAGE_FUNCTION(Table, int64_t, int)
REALM_AVERAGE_FUNCTION(TableView, int64_t, int)
REALM_AVERAGE_FUNCTION(Table, float, float)
REALM_AVERAGE_FUNCTION(TableView, double, double)

#undef REALM_AVERAGE_FUNCTION


template<typename T>
util::Optional<T> PrimitiveResults<T>::max()
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return maximum<T, T>(*table());
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return maximum<T, T>(tableview());
    }
}

template<typename T>
util::Optional<T> PrimitiveResults<T>::min()
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return minimum<T, T>(*table());
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return minimum<T, T>(tableview());
    }
}

template<typename T>
util::Optional<T> PrimitiveResults<T>::sum()
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return ::sum<T, T>(*table());
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return ::sum<T, T>(tableview());
    }
}

template<typename T>
util::Optional<double> PrimitiveResults<T>::average()
{
    switch (get_mode()) {
        default: REALM_COMPILER_HINT_UNREACHABLE();
        case Mode::Empty:
            return none;
        case Mode::Table:
            return ::average<double, T>(*table());
        case Mode::Query:
        case Mode::TableView:
            this->update_tableview();
            return ::average<double, T>(tableview());
    }
}

template<typename T>
PrimitiveResults<T> PrimitiveResults<T>::sort(Sort sort) const
{
    return PrimitiveResults(get_realm(), get_query(), sort, is_distinct());
}

template<typename T>
PrimitiveResults<T> PrimitiveResults<T>::filter(Query&& q) const
{
    return PrimitiveResults(get_realm(), get_query().and_query(std::move(q)), get_sort(), is_distinct());
}

template<typename T>
PrimitiveResults<T> PrimitiveResults<T>::distinct()
{
    auto tv = get_tableview();
    tv.distinct(0);
    return PrimitiveResults(get_realm(), std::move(tv), get_sort(), true);
}

template<typename T>
PrimitiveResults<T> PrimitiveResults<T>::snapshot() const &
{
    validate_read();
    return PrimitiveResults(*this).snapshot();
}

template<typename T>
PrimitiveResults<T> PrimitiveResults<T>::snapshot() &&
{
    snapshot();
    return std::move(*this);
}

namespace realm {
    template class PrimitiveResults<bool>;
    template class PrimitiveResults<int64_t>;
    template class PrimitiveResults<float>;
    template class PrimitiveResults<double>;
    template class PrimitiveResults<StringData>;
    template class PrimitiveResults<BinaryData>;
    template class PrimitiveResults<Timestamp>;
    template class PrimitiveResults<util::Optional<bool>>;
    template class PrimitiveResults<util::Optional<int64_t>>;
    template class PrimitiveResults<util::Optional<float>>;
    template class PrimitiveResults<util::Optional<double>>;
}
