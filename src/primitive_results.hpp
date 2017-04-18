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

#ifndef REALM_OS_PRIMITIVE_RESULTS_HPP
#define REALM_OS_PRIMITIVE_RESULTS_HPP

#include "impl/results_base.hpp"

namespace realm {
class PrimitiveResults : public _impl::ResultsBase {
public:
    enum class Sort {
        None,
        Ascending,
        Descending
    };

    // PrimitiveResults can be either be backed by nothing, a thin wrapper around a table,
    // or a wrapper around a query and a sort order which creates and updates
    // the tableview as needed
    PrimitiveResults();
    PrimitiveResults(std::shared_ptr<Realm> r, Table& table);
    PrimitiveResults(std::shared_ptr<Realm> r, Query q, Sort s = {}, bool d = {});
    PrimitiveResults(std::shared_ptr<Realm> r, TableView tv, Sort s = {}, bool d = {});
    ~PrimitiveResults();

    // PrimitiveResults is copyable and moveable
    PrimitiveResults(PrimitiveResults&&);
    PrimitiveResults& operator=(PrimitiveResults&&);
    PrimitiveResults(const PrimitiveResults&);
    PrimitiveResults& operator=(const PrimitiveResults&);

    // Get the currently applied sort order for this PrimitiveResults
    Sort get_sort() const noexcept { return m_sort; }

    // Get the currently applied distinct condition for this PrimitiveResults
    bool is_distinct() const noexcept;

    // Get the row accessor for the given index
    // Throws OutOfBoundsIndexException if index >= size()
    template<typename T> T get(size_t index);

    // Get a row accessor for the first/last row, or none if the results are empty
    // More efficient than calling size()+get()
    template<typename T> util::Optional<T> first();
    template<typename T> util::Optional<T> last();

    // Get the first index of the given value in this results, or not_found
    template<typename T> size_t index_of(T value);

    // Create a new PrimitiveResults by further filtering or sorting this PrimitiveResults
    PrimitiveResults filter(Query&& q) const;
    PrimitiveResults sort(Sort sort) const;

    // Create a new PrimitiveResults by removing duplicates
    PrimitiveResults distinct();

    // Return a snapshot of this PrimitiveResults that never updates to reflect
    // changes in the underlying data
    PrimitiveResults snapshot() const&;
    PrimitiveResults snapshot() &&;

    // Get the min/max/average/sum of the given column
    // All but sum() returns none when there are zero matching rows
    // sum() returns 0, except for when it returns none
    // Throws UnsupportedColumnTypeException for sum/average on timestamp or non-numeric column
    // Throws OutOfBoundsIndexException for an out-of-bounds column
    template<typename T> util::Optional<T> max();
    template<typename T> util::Optional<T> min();
    template<typename T> util::Optional<double> average();
    template<typename T> util::Optional<T> sum();

    template<typename Context> auto get(Context&, size_t index);
    template<typename Context> auto first(Context&);
    template<typename Context> auto last(Context&);

    template<typename Context, typename T>
    size_t index_of(Context&, T value);

    template<typename Context> auto max(Context&);
    template<typename Context> auto min(Context&);
    template<typename Context> auto average(Context&);
    template<typename Context> auto sum(Context&);

private:
    Sort m_sort = Sort::None;
};
}

#endif // REALM_OS_PRIMITIVE_RESULTS_HPP
