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

#include "sync/sync_permission.hpp"

#include "impl/object_accessor_impl.hpp"
#include "object_schema.hpp"
#include "property.hpp"

#include "sync/sync_config.hpp"
#include "sync/sync_manager.hpp"
#include "sync/sync_session.hpp"
#include "sync/sync_user.hpp"
#include "util/event_loop_signal.hpp"
#include "util/uuid.hpp"

#include <realm/query_expression.hpp>
#include <realm/util/to_string.hpp>

using namespace realm;

// MARK: - Utility

namespace {

Permission::AccessLevel extract_access_level(Object& permission, CppContext& context)
{
    auto may_manage = permission.get_property_value<util::Any>(&context, "mayManage");
    if (may_manage.has_value() && any_cast<bool>(may_manage))
        return Permission::AccessLevel::Admin;

    auto may_write = permission.get_property_value<util::Any>(&context, "mayWrite");
    if (may_write.has_value() && any_cast<bool>(may_write))
        return Permission::AccessLevel::Write;

    auto may_read = permission.get_property_value<util::Any>(&context, "mayRead");
    if (may_read.has_value() && any_cast<bool>(may_read))
        return Permission::AccessLevel::Read;

    return Permission::AccessLevel::None;
}

}

// MARK: - PermissionResults

Permission PermissionResults::get(size_t index)
{
    Object permission(m_results.get_realm(), m_results.get_object_schema(), m_results.get(index));
    CppContext context;
    return {
        any_cast<std::string>(permission.get_property_value<util::Any>(&context, "path")),
        extract_access_level(permission, context),
        { any_cast<std::string>(permission.get_property_value<util::Any>(&context, "userId")) }
    };
}

PermissionResults PermissionResults::filter(REALM_UNUSED Query&& q) const
{
    throw new std::runtime_error("not yet supported");
}

// MARK: - Permissions

// A box that stores a value and an associated notification token.
// The point of this type is to keep the notification token alive
// until the value can be properly processed or handled.
template<typename T>
struct LifetimeProlongingBox {
    T value;
    NotificationToken token;
};

void Permissions::get_permissions(std::shared_ptr<SyncUser> user,
                                  std::function<void(std::unique_ptr<PermissionResults>, std::exception_ptr)> callback,
                                  const ConfigMaker& make_config)
{
    auto realm = Permissions::permission_realm(user, make_config);
    auto session = SyncManager::shared().get_session(realm->config().path, *realm->config().sync_config);
    struct SignalBox {
        std::shared_ptr<EventLoopSignal<std::function<void()>>> ptr = {};
    };
    // `signal_box` exists so that `signal`'s lifetime can be prolonged until the results
    // callback constructed below is done executing.
    auto signal_box = std::make_shared<SignalBox>();
    auto signal = std::make_shared<EventLoopSignal<std::function<void()>>>([make_config,
                                                                            user=std::move(user),
                                                                            signal_box=signal_box,
                                                                            callback=std::move(callback)](){
        auto results_box = std::make_shared<LifetimeProlongingBox<Results>>();
        auto realm = Permissions::permission_realm(user, make_config);

        TableRef table = ObjectStore::table_for_object_type(realm->read_group(), "Permission");
        results_box->value = Results(std::move(realm), *table);
        auto async = [results_box, callback=std::move(callback)](auto ex) mutable {
            Results& res = results_box->value;
            if (ex) {
                callback(nullptr, ex);
                results_box.reset();
            } else if (res.size() > 0) {
                // We monitor the raw results. The presence of a `__management` Realm indicates
                // that the permissions have been downloaded (hence, we wait until size > 0).
                TableRef table = ObjectStore::table_for_object_type(res.get_realm()->read_group(), "Permission");
                size_t col_idx = table->get_descriptor()->get_column_index("path");
                auto query = !(table->column<StringData>(col_idx).ends_with("/__permission")
                               || table->column<StringData>(col_idx).ends_with("/__management"));
                callback(std::make_unique<PermissionResults>(res.filter(std::move(query))), nullptr);
                results_box.reset();
            }
        };
        signal_box->ptr.reset();
        results_box->token = results_box->value.async(std::move(async));
    });
    signal_box->ptr = signal;
    // Wait for download completion and then notify the run loop.
    session->wait_for_download_completion([signal=std::move(signal)](auto) {
        signal->notify();
    });
}

void Permissions::set_permission(std::shared_ptr<SyncUser> user,
                                 Permission permission,
                                 PermissionChangeCallback callback,
                                 const ConfigMaker& make_config)
{
    const auto realm_url = user->server_url() + permission.path;
    auto realm = Permissions::management_realm(std::move(user), make_config);
    auto object_box = std::make_shared<LifetimeProlongingBox<Object>>();
    CppContext context;

    // Write the permission object.
    realm->begin_transaction();
    object_box->value = Object::create<util::Any>(&context, realm, *realm->schema().find("PermissionChange"), AnyDict{
        { "id",         util::uuid_string() },
        { "createdAt",  Timestamp(0, 0) },
        { "updatedAt",  Timestamp(0, 0) },
        { "userId",     permission.condition.user_id },
        { "realmUrl",   realm_url },
        { "mayRead",    permission.access != Permission::AccessLevel::None },
        { "mayWrite",   permission.access == Permission::AccessLevel::Write || permission.access == Permission::AccessLevel::Admin },
        { "mayManage",  permission.access == Permission::AccessLevel::Admin },
    }, false);
    realm->commit_transaction();

    auto block = [object_box, callback=std::move(callback)](auto, std::exception_ptr ex) mutable {
        Object& obj = object_box->value;
        if (ex) {
            callback(ex);
            object_box.reset();
            return;
        }
        CppContext context;
        auto status_code = obj.get_property_value<util::Any>(&context, "statusCode");
        if (status_code.has_value()) {
            auto code = any_cast<long long>(status_code);
            std::exception_ptr exc_ptr = nullptr;
            if (code) {
                auto status = obj.get_property_value<util::Any>(&context, "statusMessage");
                std::string error_str = (status.has_value()
                                         ? any_cast<std::string>(status)
                                         : std::string("Error code: ") + util::to_string(code));
                exc_ptr = std::make_exception_ptr(PermissionChangeException(error_str, code));
            }
            callback(exc_ptr);
            object_box.reset();
        }
    };
    object_box->token = object_box->value.add_notification_block(std::move(block));
}

void Permissions::delete_permission(std::shared_ptr<SyncUser> user,
                                    Permission permission,
                                    PermissionChangeCallback callback,
                                    const ConfigMaker& make_config)
{
    permission.access = Permission::AccessLevel::None;
    set_permission(std::move(user), std::move(permission), std::move(callback), make_config);
}

SharedRealm Permissions::management_realm(std::shared_ptr<SyncUser> user, const ConfigMaker& make_config)
{
    // FIXME: maybe we should cache the management Realm on the user, so we don't need to open it every time.
    const auto realm_url = std::string("realm") + user->server_url().substr(4) + "/~/__management";
    Realm::Config config = make_config(user, std::move(realm_url));
    config.sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    config.schema = Schema{
        { "PermissionChange", {
            { "id",             PropertyType::String,   "", "", true, true, false   },
            { "createdAt",      PropertyType::Date,     "", "", false, false, false },
            { "updatedAt",      PropertyType::Date,     "", "", false, false, false },
            { "statusCode",     PropertyType::Int,      "", "", false, false, true  },
            { "statusMessage",  PropertyType::String,   "", "", false, false, true  },
            { "userId",         PropertyType::String,   "", "", false, false, false },
            { "realmUrl",       PropertyType::String,   "", "", false, false, false },
            { "mayRead",        PropertyType::Bool,     "", "", false, false, true  },
            { "mayWrite",       PropertyType::Bool,     "", "", false, false, true  },
            { "mayManage",      PropertyType::Bool,     "", "", false, false, true  },
        }}
    };
    config.schema_version = 0;
    auto shared_realm = Realm::get_shared_realm(std::move(config));
    user->register_management_session(config.path);
    return shared_realm;
}

SharedRealm Permissions::permission_realm(std::shared_ptr<SyncUser> user, const ConfigMaker& make_config)
{
    // FIXME: maybe we should cache the permission Realm on the user, so we don't need to open it every time.
    const auto realm_url = std::string("realm") + user->server_url().substr(4) + "/~/__permission";
    Realm::Config config = make_config(user, std::move(realm_url));
    config.sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    config.schema = Schema{
        { "Permission", {
            { "updatedAt",      PropertyType::Date,     "", "", false, false, false },
            { "userId",         PropertyType::String,   "", "", false, false, false },
            { "path",           PropertyType::String,   "", "", false, false, false },
            { "mayRead",        PropertyType::Bool,     "", "", false, false, false },
            { "mayWrite",       PropertyType::Bool,     "", "", false, false, false },
            { "mayManage",      PropertyType::Bool,     "", "", false, false, false },
        }}
    };
    config.schema_version = 0;
    auto shared_realm = Realm::get_shared_realm(std::move(config));
    user->register_permission_session(config.path);
    return shared_realm;
}
