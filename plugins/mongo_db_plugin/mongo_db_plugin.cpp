/**
 *  @file
 *  @copyright defined in eos/LICENSE.txt
 */
#include <eosio/chain/asset.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/eosio_contract.hpp>
#include <eosio/chain/exceptions.hpp>
#include <eosio/chain/transaction.hpp>
#include <eosio/chain/types.hpp>
#include <eosio/mongo_db_plugin/mongo_db_plugin.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <boost/chrono.hpp>
#include <boost/signals2/connection.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include <queue>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>

#include <eosio/mongo_db_plugin/contract_types.hpp>
#include <eosio/mongo_db_plugin/ram_market.hpp>

namespace fc {
class variant;
}

namespace eosio {

using chain::account_name;
using chain::action_name;
using chain::block_id_type;
using chain::block_trace;
using chain::packed_transaction;
using chain::permission_name;
using chain::signed_block;
using chain::signed_transaction;
using chain::transaction;
using chain::transaction_id_type;

static appbase::abstract_plugin& _mongo_db_plugin =
    app().register_plugin<mongo_db_plugin>();

class mongo_db_plugin_impl {
   public:
    mongo_db_plugin_impl();
    ~mongo_db_plugin_impl();

    chain_plugin* chain_plug;
    RamMarket rammarket;

    fc::optional<boost::signals2::scoped_connection> accepted_block_connection;
    fc::optional<boost::signals2::scoped_connection>
        irreversible_block_connection;
    fc::optional<boost::signals2::scoped_connection>
        accepted_transaction_connection;
    fc::optional<boost::signals2::scoped_connection>
        applied_transaction_connection;

    struct action_trace_tuple {
        const chain::transaction_trace_ptr trace;
        uint32_t block_num;
        fc::time_point block_time;
    };

    struct get_account_results {
        asset core_liquid_balance;
        asset stacked;
        asset unstacking;
        asset total;
    };

    void consume_blocks();

    void accepted_block(const chain::block_state_ptr&);
    void applied_irreversible_block(const chain::block_state_ptr&);
    void accepted_transaction(const chain::transaction_metadata_ptr&);
    void applied_transaction(const action_trace_tuple&);
    void process_accepted_transaction(const chain::transaction_metadata_ptr&);
    void _process_accepted_transaction(const chain::transaction_metadata_ptr&);
    void process_applied_transaction(const action_trace_tuple&);
    void _process_applied_transaction(const action_trace_tuple&);
    void process_accepted_block(const chain::block_state_ptr&);
    void _process_accepted_block(const chain::block_state_ptr&);
    void process_irreversible_block(const chain::block_state_ptr&);
    void _process_irreversible_block(const chain::block_state_ptr&);

    void _handle_action_trace(const chain::action_trace&, uint32_t,
                              fc::time_point);
    void _handle_system_action_trace(const chain::action_trace&, uint32_t,
                                     fc::time_point);
    void _handle_ramex_action_trace(const chain::action_trace&, uint32_t,
                                    fc::time_point);
    void _handle_setram_action_trace(const chain::action_trace&);
    void _handle_eos_token_action_trace(const chain::action_trace&, uint32_t,
                                        fc::time_point);

    void sync_account_balance(account_name account,
                              mongocxx::collection& accounts);

    void init();
    void wipe_database();

    bool configured{false};
    bool wipe_database_on_startup{false};
    uint32_t start_block_num = 0;
    bool start_block_reached = false;

    std::string db_name;
    mongocxx::instance mongo_inst;
    mongocxx::client mongo_conn;
    mongocxx::collection accounts;
    mongocxx::collection global_collection;

    size_t queue_size = 0;
    std::deque<chain::transaction_metadata_ptr> transaction_metadata_queue;
    std::deque<chain::transaction_metadata_ptr>
        transaction_metadata_process_queue;
    std::deque<action_trace_tuple> transaction_trace_queue;
    std::deque<action_trace_tuple> transaction_trace_process_queue;
    std::deque<chain::block_state_ptr> block_state_queue;
    std::deque<chain::block_state_ptr> block_state_process_queue;
    std::deque<chain::block_state_ptr> irreversible_block_state_queue;
    std::deque<chain::block_state_ptr> irreversible_block_state_process_queue;
    boost::mutex mtx;
    boost::condition_variable condition;
    boost::thread consume_thread;
    boost::atomic<bool> done{false};
    boost::atomic<bool> startup{true};
    fc::optional<chain::chain_id_type> chain_id;
    fc::microseconds abi_serializer_max_time;

    static const account_name newaccount;
    static const account_name setabi;

    static const std::string block_states_col;
    static const std::string blocks_col;
    static const std::string trans_col;
    static const std::string trans_traces_col;
    static const std::string actions_col;
    static const std::string accounts_col;
    static const std::string ram_trade_col;
    static const std::string global_col;
};

const account_name mongo_db_plugin_impl::newaccount = "newaccount";
const account_name mongo_db_plugin_impl::setabi = "setabi";

const std::string mongo_db_plugin_impl::block_states_col = "block_states";
const std::string mongo_db_plugin_impl::blocks_col = "blocks";
const std::string mongo_db_plugin_impl::trans_col = "transactions";
const std::string mongo_db_plugin_impl::trans_traces_col = "transaction_traces";
const std::string mongo_db_plugin_impl::actions_col = "actions";
const std::string mongo_db_plugin_impl::accounts_col = "accounts";
const std::string mongo_db_plugin_impl::ram_trade_col = "ram_trade";
const std::string mongo_db_plugin_impl::global_col = "global";

namespace {

template <typename Queue, typename Entry>
void queue(boost::mutex& mtx, boost::condition_variable& condition,
           Queue& queue, const Entry& e, size_t queue_size) {
    int sleep_time = 100;
    size_t last_queue_size = 0;
    boost::mutex::scoped_lock lock(mtx);
    if (queue.size() > queue_size) {
        lock.unlock();
        condition.notify_one();
        if (last_queue_size < queue.size()) {
            sleep_time += 100;
        } else {
            sleep_time -= 100;
            if (sleep_time < 0) sleep_time = 100;
        }
        last_queue_size = queue.size();
        boost::this_thread::sleep_for(boost::chrono::milliseconds(sleep_time));
        lock.lock();
    }
    queue.emplace_back(e);
    lock.unlock();
    condition.notify_one();
}

}  // namespace

void mongo_db_plugin_impl::accepted_transaction(
    const chain::transaction_metadata_ptr& t) {
    try {
        queue(mtx, condition, transaction_metadata_queue, t, queue_size);
    } catch (fc::exception& e) {
        elog("FC Exception while accepted_transaction ${e}",
             ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while accepted_transaction ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while accepted_transaction");
    }
}

void mongo_db_plugin_impl::applied_transaction(const action_trace_tuple& tp) {
    try {
        queue(mtx, condition, transaction_trace_queue, tp, queue_size);
    } catch (fc::exception& e) {
        elog("FC Exception while applied_transaction ${e}",
             ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while applied_transaction ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while applied_transaction");
    }
}

void mongo_db_plugin_impl::applied_irreversible_block(
    const chain::block_state_ptr& bs) {
    try {
        queue(mtx, condition, irreversible_block_state_queue, bs, queue_size);
    } catch (fc::exception& e) {
        elog("FC Exception while applied_irreversible_block ${e}",
             ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while applied_irreversible_block ${e}",
             ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while applied_irreversible_block");
    }
}

void mongo_db_plugin_impl::accepted_block(const chain::block_state_ptr& bs) {
    try {
        queue(mtx, condition, block_state_queue, bs, queue_size);
    } catch (fc::exception& e) {
        elog("FC Exception while accepted_block ${e}", ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while accepted_block ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while accepted_block");
    }
}

void mongo_db_plugin_impl::consume_blocks() {
    try {
        while (true) {
            boost::mutex::scoped_lock lock(mtx);
            while (transaction_metadata_queue.empty() &&
                   transaction_trace_queue.empty() &&
                   block_state_queue.empty() &&
                   irreversible_block_state_queue.empty() && !done) {
                condition.wait(lock);
            }

            // capture for processing
            size_t transaction_metadata_size =
                transaction_metadata_queue.size();
            if (transaction_metadata_size > 0) {
                transaction_metadata_process_queue =
                    move(transaction_metadata_queue);
                transaction_metadata_queue.clear();
            }
            size_t transaction_trace_size = transaction_trace_queue.size();
            if (transaction_trace_size > 0) {
                transaction_trace_process_queue = move(transaction_trace_queue);
                transaction_trace_queue.clear();
            }
            size_t block_state_size = block_state_queue.size();
            if (block_state_size > 0) {
                block_state_process_queue = move(block_state_queue);
                block_state_queue.clear();
            }
            size_t irreversible_block_size =
                irreversible_block_state_queue.size();
            if (irreversible_block_size > 0) {
                irreversible_block_state_process_queue =
                    move(irreversible_block_state_queue);
                irreversible_block_state_queue.clear();
            }

            lock.unlock();

            // warn if queue size greater than 75%
            if (transaction_metadata_size > (queue_size * 0.75) ||
                transaction_trace_size > (queue_size * 0.75) ||
                block_state_size > (queue_size * 0.75) ||
                irreversible_block_size > (queue_size * 0.75)) {
                wlog("queue size: ${q}",
                     ("q", transaction_metadata_size + transaction_trace_size +
                               block_state_size + irreversible_block_size));
            } else if (done) {
                ilog("draining queue, size: ${q}",
                     ("q", transaction_metadata_size + transaction_trace_size +
                               block_state_size + irreversible_block_size));
            }

            // process transactions
            while (!transaction_metadata_process_queue.empty()) {
                const auto& t = transaction_metadata_process_queue.front();
                process_accepted_transaction(t);
                transaction_metadata_process_queue.pop_front();
            }

            while (!transaction_trace_process_queue.empty()) {
                const auto& t = transaction_trace_process_queue.front();
                process_applied_transaction(t);
                transaction_trace_process_queue.pop_front();
            }

            // process blocks
            while (!block_state_process_queue.empty()) {
                const auto& bs = block_state_process_queue.front();
                process_accepted_block(bs);
                block_state_process_queue.pop_front();
            }

            // process irreversible blocks
            while (!irreversible_block_state_process_queue.empty()) {
                const auto& bs = irreversible_block_state_process_queue.front();
                process_irreversible_block(bs);
                irreversible_block_state_process_queue.pop_front();
            }

            if (transaction_metadata_size == 0 && transaction_trace_size == 0 &&
                block_state_size == 0 && irreversible_block_size == 0 && done) {
                break;
            }
        }
        ilog("mongo_db_plugin consume thread shutdown gracefully");
    } catch (fc::exception& e) {
        elog("FC Exception while consuming block ${e}", ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while consuming block ${e}", ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while consuming block");
    }
}

void mongo_db_plugin_impl::sync_account_balance(
    account_name account, mongocxx::collection& accounts) {
    using namespace eosio::chain;
    using namespace bsoncxx::types;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    auto& chain = chain_plug->chain();
    const auto& db = chain.db();

    auto core_liquid_balance = asset(0, symbol(CORE_SYMBOL));
    auto unstacking = asset(0, symbol(CORE_SYMBOL));
    auto stacked = asset(0, symbol(CORE_SYMBOL));

    const auto token_code = N(eosio.token);

    const auto* t_id =
        db.find<chain::table_id_object, chain::by_code_scope_table>(
            boost::make_tuple(token_code, account, N(accounts)));
    if (t_id != nullptr) {
        const auto& idx = db.get_index<key_value_index, by_scope_primary>();
        auto it =
            idx.find(boost::make_tuple(t_id->id, symbol().to_symbol_code()));
        if (it != idx.end() && it->value.size() >= sizeof(asset)) {
            asset bal;
            fc::datastream<const char*> ds(it->value.data(), it->value.size());
            fc::raw::unpack(ds, bal);

            if (bal.get_symbol().valid() && bal.get_symbol() == symbol()) {
                core_liquid_balance = bal;
            }
        }
    }

    t_id = db.find<chain::table_id_object, chain::by_code_scope_table>(
        boost::make_tuple(config::system_account_name, account, N(delband)));
    if (t_id != nullptr) {
        const auto& idx = db.get_index<key_value_index, by_scope_primary>();
        auto it = idx.find(boost::make_tuple(t_id->id, account));
        if (it != idx.end()) {
            delegated_bandwidth bw;
            fc::datastream<const char*> ds(it->value.data(), it->value.size());
            fc::raw::unpack(ds, bw);
            stacked = bw.net_weight + bw.cpu_weight;
        }
    }

    t_id = db.find<chain::table_id_object, chain::by_code_scope_table>(
        boost::make_tuple(config::system_account_name, account, N(refunds)));
    if (t_id != nullptr) {
        const auto& idx = db.get_index<key_value_index, by_scope_primary>();
        auto it = idx.find(boost::make_tuple(t_id->id, account));
        if (it != idx.end()) {
            refund_request refund;
            fc::datastream<const char*> ds(it->value.data(), it->value.size());
            fc::raw::unpack(ds, refund);
            unstacking = refund.net_amount + refund.cpu_amount;
        }
    }

    auto total = core_liquid_balance + unstacking + stacked;

    auto update_doc = make_document(
        kvp("liquid_eos", b_int64{core_liquid_balance.get_amount()}),
        kvp("stacked_eos", b_int64{stacked.get_amount()}),
        kvp("unstacking_eos", b_int64{unstacking.get_amount()}),
        kvp("total_eos", b_int64{total.get_amount()}));

    accounts.update_one(make_document(kvp("name", account.to_string())),
                        make_document(kvp("$set", update_doc)));
}

namespace {

auto find_account(mongocxx::collection& accounts, const account_name& name) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    return accounts.find_one(make_document(kvp("name", name.to_string())));
}

auto find_transaction(mongocxx::collection& trans, const string& id) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    return trans.find_one(make_document(kvp("trx_id", id)));
}

auto find_block(mongocxx::collection& blocks, const string& id) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    return blocks.find_one(make_document(kvp("block_id", id)));
}

optional<abi_serializer> get_abi_serializer(
    account_name n, mongocxx::collection& accounts,
    const fc::microseconds& abi_serializer_max_time) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    if (n.good()) {
        try {
            auto account =
                accounts.find_one(make_document(kvp("name", n.to_string())));
            if (account) {
                auto view = account->view();
                abi_def abi;
                if (view.find("abi") != view.end()) {
                    try {
                        abi = fc::json::from_string(
                                  bsoncxx::to_json(view["abi"].get_document()))
                                  .as<abi_def>();
                    } catch (...) {
                        ilog(
                            "Unable to convert account abi to abi_def for ${n}",
                            ("n", n));
                        return optional<abi_serializer>();
                    }
                    return abi_serializer(abi, abi_serializer_max_time);
                }
            }
        }
        FC_CAPTURE_AND_LOG((n))
    }
    return optional<abi_serializer>();
}

template <typename T>
fc::variant to_variant_with_abi(
    const T& obj, mongocxx::collection& accounts,
    const fc::microseconds& abi_serializer_max_time) {
    fc::variant pretty_output;
    abi_serializer::to_variant(obj, pretty_output,
                               [&](account_name n) {
                                   return get_abi_serializer(
                                       n, accounts, abi_serializer_max_time);
                               },
                               abi_serializer_max_time);
    return pretty_output;
}

void handle_mongo_exception(const std::string& desc, int line_num) {
    bool shutdown = true;
    try {
        try {
            throw;
        } catch (mongocxx::logic_error& e) {
            // logic_error on invalid key, do not shutdown
            wlog(
                "mongo logic error, ${desc}, line ${line}, code ${code}, "
                "${what}",
                ("desc", desc)("line", line_num)("code", e.code().value())(
                    "what", e.what()));
            shutdown = false;
        } catch (mongocxx::operation_exception& e) {
            elog(
                "mongo exception, ${desc}, line ${line}, code ${code}, "
                "${details}",
                ("desc", desc)("line", line_num)("code", e.code().value())(
                    "details", e.code().message()));
            if (e.raw_server_error()) {
                elog("mongo exception, ${desc}, line ${line}, ${details}",
                     ("desc", desc)("line", line_num)(
                         "details",
                         bsoncxx::to_json(e.raw_server_error()->view())));
            }
        } catch (mongocxx::exception& e) {
            elog(
                "mongo exception, ${desc}, line ${line}, code ${code}, ${what}",
                ("desc", desc)("line", line_num)("code", e.code().value())(
                    "what", e.what()));
        } catch (bsoncxx::exception& e) {
            elog(
                "bsoncxx exception, ${desc}, line ${line}, code ${code}, "
                "${what}",
                ("desc", desc)("line", line_num)("code", e.code().value())(
                    "what", e.what()));
        } catch (fc::exception& er) {
            elog("mongo fc exception, ${desc}, line ${line}, ${details}",
                 ("desc", desc)("line", line_num)("details",
                                                  er.to_detail_string()));
        } catch (const std::exception& e) {
            elog("mongo std exception, ${desc}, line ${line}, ${what}",
                 ("desc", desc)("line", line_num)("what", e.what()));
        } catch (...) {
            elog("mongo unknown exception, ${desc}, line ${line_nun}",
                 ("desc", desc)("line_num", line_num));
        }
    } catch (...) {
        std::cerr << "Exception attempting to handle exception for " << desc
                  << " " << line_num << std::endl;
    }

    if (shutdown) {
        // shutdown if mongo failed to provide opportunity to fix issue and
        // restart
        app().quit();
    }
}

void update_account(mongocxx::collection& accounts, const chain::action& act,
                    chain_plugin* chain_plug) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    using namespace bsoncxx::types;

    if (act.account != chain::config::system_account_name) return;

    try {
        if (act.name == mongo_db_plugin_impl::newaccount) {
            // auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            //     std::chrono::microseconds{
            //         fc::time_point::now().time_since_epoch().count()});
            auto newaccount = act.data_as<chain::newaccount>();

            auto& chain = chain_plug->chain();
            const auto& a = chain.get_account(newaccount.name);
            auto create_t =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::microseconds{a.creation_date.to_time_point()
                                                  .time_since_epoch()
                                                  .count()});

            // create new account
            if (!accounts.insert_one(
                    make_document(kvp("name", newaccount.name.to_string()),
                                  kvp("create_time", b_date{create_t})))) {
                elog("Failed to insert account ${n}", ("n", newaccount.name));
            }

        } else if (act.name == mongo_db_plugin_impl::setabi) {
            auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::microseconds{
                    fc::time_point::now().time_since_epoch().count()});
            auto setabi = act.data_as<chain::setabi>();
            auto from_account = find_account(accounts, setabi.account);
            if (!from_account) {
                auto& chain = chain_plug->chain();
                const auto& a = chain.get_account(setabi.account);
                auto create_t =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::microseconds{
                            a.creation_date.to_time_point()
                                .time_since_epoch()
                                .count()});

                if (!accounts.insert_one(
                        make_document(kvp("name", setabi.account.to_string()),
                                      kvp("create_time", b_date{create_t})))) {
                    elog("Failed to insert account ${n}",
                         ("n", setabi.account));
                }
                from_account = find_account(accounts, setabi.account);
            }
            // if (from_account) {
            //     const abi_def& abi_def =
            //         fc::raw::unpack<chain::abi_def>(setabi.abi);
            //     const string json_str = fc::json::to_string(abi_def);

            //     try {
            //         auto update_from = make_document(kvp(
            //             "$set",
            //             make_document(kvp("abi",
            //             bsoncxx::from_json(json_str)),
            //                           kvp("updatedAt", b_date{now}))));

            //         try {
            //             if (!accounts.update_one(
            //                     make_document(
            //                         kvp("_id",
            //                             from_account->view()["_id"].get_oid())),
            //                     update_from.view())) {
            //                 EOS_ASSERT(false, chain::mongo_db_update_fail,
            //                            "Failed to udpdate account ${n}",
            //                            ("n", setabi.account));
            //             }
            //         } catch (...) {
            //             handle_mongo_exception("account update", __LINE__);
            //         }
            //     } catch (bsoncxx::exception& e) {
            //         elog("Unable to convert abi JSON to MongoDB JSON: ${e}",
            //              ("e", e.what()));
            //         elog("  JSON: ${j}", ("j", json_str));
            //     }
            // }
        }
    } catch (fc::exception& e) {
        // if unable to unpack native type, skip account creation
    }
}

void add_data(bsoncxx::builder::basic::document& act_doc,
              mongocxx::collection& accounts, const chain::action& act,
              const fc::microseconds& abi_serializer_max_time) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    try {
        if (act.account == chain::config::system_account_name) {
            if (act.name == mongo_db_plugin_impl::setabi) {
                auto setabi = act.data_as<chain::setabi>();
                try {
                    const abi_def& abi_def =
                        fc::raw::unpack<chain::abi_def>(setabi.abi);
                    const string json_str = fc::json::to_string(abi_def);

                    act_doc.append(
                        kvp("data",
                            make_document(
                                kvp("account", setabi.account.to_string()),
                                kvp("abi_def", bsoncxx::from_json(json_str)))));
                    return;
                } catch (bsoncxx::exception&) {
                    // better error handling below
                } catch (fc::exception& e) {
                    ilog("Unable to convert action abi_def to json for ${n}",
                         ("n", setabi.account.to_string()));
                }
            }
        }
        auto account = find_account(accounts, act.account);
        if (account) {
            auto from_account = *account;
            abi_def abi;
            if (from_account.view().find("abi") != from_account.view().end()) {
                try {
                    abi = fc::json::from_string(
                              bsoncxx::to_json(
                                  from_account.view()["abi"].get_document()))
                              .as<abi_def>();
                } catch (...) {
                    ilog(
                        "Unable to convert account abi to abi_def for "
                        "${s}::${n}",
                        ("s", act.account)("n", act.name));
                }
            }
            string json;
            try {
                abi_serializer abis;
                abis.set_abi(abi, abi_serializer_max_time);
                auto v =
                    abis.binary_to_variant(abis.get_action_type(act.name),
                                           act.data, abi_serializer_max_time);
                json = fc::json::to_string(v);

                const auto& value = bsoncxx::from_json(json);
                act_doc.append(kvp("data", value));
                return;
            } catch (bsoncxx::exception& e) {
                ilog("Unable to convert EOS JSON to MongoDB JSON: ${e}",
                     ("e", e.what()));
                ilog("  EOS JSON: ${j}", ("j", json));
                ilog("  Storing data has hex.");
            }
        }
    } catch (std::exception& e) {
        ilog("Unable to convert action.data to ABI: ${s}::${n}, std what: ${e}",
             ("s", act.account)("n", act.name)("e", e.what()));
    } catch (fc::exception& e) {
        if (act.name !=
            "onblock") {  // eosio::onblock not in original eosio.system abi
            ilog(
                "Unable to convert action.data to ABI: ${s}::${n}, fc "
                "exception: ${e}",
                ("s", act.account)("n", act.name)("e", e.to_detail_string()));
        }
    } catch (...) {
        ilog(
            "Unable to convert action.data to ABI: ${s}::${n}, unknown "
            "exception",
            ("s", act.account)("n", act.name));
    }
    // if anything went wrong just store raw hex_data
    act_doc.append(kvp("hex_data", fc::variant(act.data).as_string()));
}

void update_rammarket_to_db(mongocxx::collection& global,
                            const RamMarket& market) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    mongocxx::options::update update_opts{};
    update_opts.upsert(true);

    auto market_doc = make_document(
        kvp("supply", market.supply.to_string()),
        kvp("base",
            make_document(kvp("balance", market.base.balance.to_string()),
                          kvp("weight", fc::to_string(market.base.weight)))),
        kvp("quote",
            make_document(kvp("balance", market.quote.balance.to_string()),
                          kvp("weight", fc::to_string(market.quote.weight)))));

    try {
        global.update_one(
            make_document(kvp("key", "rammarket")),
            make_document(
                kvp("$set", make_document(kvp("value", market_doc)).view())),
            update_opts);
    } catch (...) {
        handle_mongo_exception("rammarket update", __LINE__);
    }
}

RamMarket get_rammarket_from_db(mongocxx::collection& global) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    RamMarket market{};
    auto marketDoc = global.find_one(make_document(kvp("key", "rammarket")));
    if (marketDoc) {
        auto view = marketDoc->view();
        const fc::variant& val = fc::json::from_string(
            bsoncxx::to_json(view["value"].get_document()));
        market = RamMarket(val);
    } else {
        update_rammarket_to_db(global, market);
    }

    return market;
}

}  // anonymous namespace

void mongo_db_plugin_impl::process_accepted_transaction(
    const chain::transaction_metadata_ptr& t) {
    try {
        // always call since we need to capture setabi on accounts even if not
        // storing transactions
        _process_accepted_transaction(t);
    } catch (fc::exception& e) {
        elog(
            "FC Exception while processing accepted transaction metadata: ${e}",
            ("e", e.to_detail_string()));
    } catch (std::exception& e) {
        elog(
            "STD Exception while processing accepted tranasction metadata: "
            "${e}",
            ("e", e.what()));
    } catch (...) {
        elog(
            "Unknown exception while processing accepted transaction metadata");
    }
}

void mongo_db_plugin_impl::process_applied_transaction(
    const action_trace_tuple& tp) {
    try {
        if (start_block_reached) {
            _process_applied_transaction(tp);
        }
    } catch (fc::exception& e) {
        elog("FC Exception while processing applied transaction trace: ${e}",
             ("e", e.to_detail_string()));
    } catch (std::exception& e) {
        elog("STD Exception while processing applied transaction trace: ${e}",
             ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while processing applied transaction trace");
    }
}

void mongo_db_plugin_impl::process_irreversible_block(
    const chain::block_state_ptr& bs) {
    try {
        if (start_block_reached) {
            _process_irreversible_block(bs);
        }
    } catch (fc::exception& e) {
        elog("FC Exception while processing irreversible block: ${e}",
             ("e", e.to_detail_string()));
    } catch (std::exception& e) {
        elog("STD Exception while processing irreversible block: ${e}",
             ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while processing irreversible block");
    }
}

void mongo_db_plugin_impl::process_accepted_block(
    const chain::block_state_ptr& bs) {
    try {
        if (!start_block_reached) {
            if (bs->block_num >= start_block_num) {
                start_block_reached = true;
            }
        }
        if (start_block_reached) {
            _process_accepted_block(bs);
        }
    } catch (fc::exception& e) {
        elog("FC Exception while processing accepted block trace ${e}",
             ("e", e.to_string()));
    } catch (std::exception& e) {
        elog("STD Exception while processing accepted block trace ${e}",
             ("e", e.what()));
    } catch (...) {
        elog("Unknown exception while processing accepted block trace");
    }
}

void mongo_db_plugin_impl::_process_accepted_transaction(
    const chain::transaction_metadata_ptr& t) {
    using namespace bsoncxx::types;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_array;
    using bsoncxx::builder::basic::make_document;
    namespace bbb = bsoncxx::builder::basic;

    auto trans = mongo_conn[db_name][trans_col];
    auto actions = mongo_conn[db_name][actions_col];
    accounts = mongo_conn[db_name][accounts_col];
    auto trans_doc = bsoncxx::builder::basic::document{};

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds{
            fc::time_point::now().time_since_epoch().count()});

    const auto trx_id = t->id;
    const auto trx_id_str = trx_id.str();
    const auto& trx = t->trx;
    //     const chain::transaction_header& trx_header = trx;

    //     bool actions_to_write = false;
    //     mongocxx::options::bulk_write bulk_opts;
    //     bulk_opts.ordered(false);
    //     mongocxx::bulk_write bulk_actions =
    //     actions.create_bulk_write(bulk_opts);

    int32_t act_num = 0;
    auto process_action = [&](const std::string& trx_id_str,
                              const chain::action& act, bbb::array& act_array,
                              bool cfa) -> auto {
        //   auto act_doc = bsoncxx::builder::basic::document();
        //   if (start_block_reached) {
        //       act_doc.append(kvp("action_num", b_int32{act_num}),
        //                      kvp("trx_id", trx_id_str));
        //       act_doc.append(kvp("cfa", b_bool{cfa}));
        //       act_doc.append(kvp("account", act.account.to_string()));
        //       act_doc.append(kvp("name", act.name.to_string()));
        //       act_doc.append(kvp(
        //           "authorization",
        //           [&act](bsoncxx::builder::basic::sub_array subarr) {
        //               for (const auto& auth : act.authorization) {
        //                   subarr.append(
        //                       [&auth](
        //                           bsoncxx::builder::basic::sub_document
        //                           subdoc) { subdoc.append(
        //                               kvp("actor", auth.actor.to_string()),
        //                               kvp("permission",
        //                                   auth.permission.to_string()));
        //                       });
        //               }
        //           }));
        //   }
        try {
            update_account(accounts, act, chain_plug);
        } catch (...) {
            ilog("Unable to update account for ${s}::${n}",
                 ("s", act.account)("n", act.name));
        }
        //   if (start_block_reached) {
        //       add_data(act_doc, accounts, act, abi_serializer_max_time);
        //       act_array.append(act_doc);
        //       mongocxx::model::insert_one insert_op{act_doc.view()};
        //       bulk_actions.append(insert_op);
        //       actions_to_write = true;
        //   }
        ++act_num;
        return act_num;
    };

    //     if (start_block_reached) {
    //         trans_doc.append(kvp("trx_id", trx_id_str),
    //                          kvp("irreversible", b_bool{false}));

    //         string signing_keys_json;
    //         if (t->signing_keys.valid()) {
    //             signing_keys_json =
    //             fc::json::to_string(t->signing_keys->second);
    //         } else {
    //             auto signing_keys = trx.get_signature_keys(*chain_id, false,
    //             false); if (!signing_keys.empty()) {
    //                 signing_keys_json = fc::json::to_string(signing_keys);
    //             }
    //         }
    //         string trx_header_json = fc::json::to_string(trx_header);

    //         try {
    //             const auto& trx_header_value =
    //             bsoncxx::from_json(trx_header_json);
    //             trans_doc.append(kvp("transaction_header",
    //             trx_header_value));
    //         } catch (bsoncxx::exception&) {
    //             try {
    //                 trx_header_json =
    //                 fc::prune_invalid_utf8(trx_header_json); const auto&
    //                 trx_header_value =
    //                     bsoncxx::from_json(trx_header_json);
    //                 trans_doc.append(kvp("transaction_header",
    //                 trx_header_value));
    //                 trans_doc.append(kvp("non-utf8-purged", b_bool{true}));
    //             } catch (bsoncxx::exception& e) {
    //                 elog(
    //                     "Unable to convert transaction header JSON to MongoDB
    //                     " "JSON: ${e}",
    //                     ("e", e.what()));
    //                 elog("  JSON: ${j}", ("j", trx_header_json));
    //             }
    //         }
    //         if (!signing_keys_json.empty()) {
    //             try {
    //                 const auto& keys_value =
    //                 bsoncxx::from_json(signing_keys_json);
    //                 trans_doc.append(kvp("signing_keys", keys_value));
    //             } catch (bsoncxx::exception& e) {
    //                 // should never fail, so don't attempt to remove invalid
    //                 utf8 elog(
    //                     "Unable to convert signing keys JSON to MongoDB JSON:
    //                     ${e}",
    //                     ("e", e.what()));
    //                 elog("  JSON: ${j}", ("j", signing_keys_json));
    //             }
    //         }
    //     }

    if (!trx.actions.empty()) {
        bsoncxx::builder::basic::array action_array;
        for (const auto& act : trx.actions) {
            process_action(trx_id_str, act, action_array, false);
        }
        //   trans_doc.append(kvp("actions", action_array));
    }

    if (start_block_reached) {
        act_num = 0;
        if (!trx.context_free_actions.empty()) {
            bsoncxx::builder::basic::array action_array;
            for (const auto& cfa : trx.context_free_actions) {
                process_action(trx_id_str, cfa, action_array, true);
            }
            // trans_doc.append(kvp("context_free_actions", action_array));
        }

        //   string trx_extensions_json =
        //       fc::json::to_string(trx.transaction_extensions);
        //   string trx_signatures_json = fc::json::to_string(trx.signatures);
        //   string trx_context_free_data_json =
        //       fc::json::to_string(trx.context_free_data);

        //   try {
        //       if (!trx_extensions_json.empty()) {
        //           try {
        //               const auto& trx_extensions_value =
        //                   bsoncxx::from_json(trx_extensions_json);
        //               trans_doc.append(
        //                   kvp("transaction_extensions",
        //                   trx_extensions_value));
        //           } catch (bsoncxx::exception&) {
        //               static_assert(sizeof(std::remove_pointer<decltype(
        //                                        b_binary::bytes)>::type) ==
        //                                 sizeof(std::string::value_type),
        //                             "string type not storable as b_binary");
        //               trans_doc.append(kvp(
        //                   "transaction_extensions",
        //                   b_binary{
        //                       bsoncxx::binary_sub_type::k_binary,
        //                       static_cast<uint32_t>(trx_extensions_json.size()),
        //                       reinterpret_cast<const u_int8_t*>(
        //                           trx_extensions_json.data())}));
        //           }
        //       } else {
        //           trans_doc.append(kvp("transaction_extensions",
        //           make_array()));
        //       }

        //       if (!trx_signatures_json.empty()) {
        //           // signatures contain only utf8
        //           const auto& trx_signatures_value =
        //               bsoncxx::from_json(trx_signatures_json);
        //           trans_doc.append(kvp("signatures", trx_signatures_value));
        //       } else {
        //           trans_doc.append(kvp("signatures", make_array()));
        //       }

        //       if (!trx_context_free_data_json.empty()) {
        //           try {
        //               const auto& trx_context_free_data_value =
        //                   bsoncxx::from_json(trx_context_free_data_json);
        //               trans_doc.append(
        //                   kvp("context_free_data",
        //                   trx_context_free_data_value));
        //           } catch (bsoncxx::exception&) {
        //               static_assert(
        //                   sizeof(std::remove_pointer<decltype(
        //                              b_binary::bytes)>::type) ==
        //                       sizeof(std::remove_pointer<decltype(
        //                                  trx.context_free_data[0].data())>::type),
        //                   "context_free_data not storable as b_binary");
        //               bsoncxx::builder::basic::array data_array;
        //               for (auto& cfd : trx.context_free_data) {
        //                   data_array.append(b_binary{
        //                       bsoncxx::binary_sub_type::k_binary,
        //                       static_cast<uint32_t>(cfd.size()),
        //                       reinterpret_cast<const
        //                       u_int8_t*>(cfd.data())});
        //               }
        //               trans_doc.append(
        //                   kvp("context_free_data", data_array.view()));
        //           }
        //       } else {
        //           trans_doc.append(kvp("context_free_data", make_array()));
        //       }
        //   } catch (std::exception& e) {
        //       elog("Unable to convert transaction JSON to MongoDB JSON:
        //       ${e}",
        //            ("e", e.what()));
        //       elog("  JSON: ${j}", ("j", trx_extensions_json));
        //       elog("  JSON: ${j}", ("j", trx_signatures_json));
        //       elog("  JSON: ${j}", ("j", trx_context_free_data_json));
        //   }

        //   trans_doc.append(kvp("createdAt", b_date{now}));
        // TODO: ignore trans_col
        // try {
        //    if( !trans.insert_one( trans_doc.view())) {
        //       EOS_ASSERT( false, chain::mongo_db_insert_fail, "Failed to
        //       insert trans ${id}", ("id", trx_id));
        //    }
        // } catch(...) {
        //    handle_mongo_exception("trans insert", __LINE__);
        // }

        //   if (actions_to_write) {
        //       try {
        //           if (!bulk_actions.execute()) {
        //               EOS_ASSERT(
        //                   false, chain::mongo_db_insert_fail,
        //                   "Bulk actions insert failed for transaction:
        //                   ${id}",
        //                   ("id", trx_id_str));
        //           }
        //       } catch (...) {
        //           handle_mongo_exception("actions insert", __LINE__);
        //       }
        //   }
    }
}

void mongo_db_plugin_impl::_process_applied_transaction(
    const action_trace_tuple& tp) {
    using namespace bsoncxx::types;
    using bsoncxx::builder::basic::kvp;

    const eosio::chain::transaction_trace_ptr& t = tp.trace;

    // auto trans_traces = mongo_conn[db_name][trans_traces_col];
    // auto trans_traces_doc = bsoncxx::builder::basic::document{};

    // auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
    //     std::chrono::microseconds{
    //         fc::time_point::now().time_since_epoch().count()});

    // auto v = to_variant_with_abi(*t, accounts, abi_serializer_max_time);
    // string json = fc::json::to_string(v);
    // try {
    //     const auto& value = bsoncxx::from_json(json);
    //     trans_traces_doc.append(
    //         bsoncxx::builder::concatenate_doc{value.view()});
    // } catch (bsoncxx::exception&) {
    //     try {
    //         json = fc::prune_invalid_utf8(json);
    //         const auto& value = bsoncxx::from_json(json);
    //         trans_traces_doc.append(
    //             bsoncxx::builder::concatenate_doc{value.view()});
    //         trans_traces_doc.append(kvp("non-utf8-purged", b_bool{true}));
    //     } catch (bsoncxx::exception& e) {
    //         elog("Unable to convert transaction JSON to MongoDB JSON: ${e}",
    //              ("e", e.what()));
    //         elog("  JSON: ${j}", ("j", json));
    //     }
    // }
    // trans_traces_doc.append(kvp("createdAt", b_date{now}));

    // TODO：ignore trans_traces
    //    try {
    //       if( !trans_traces.insert_one( trans_traces_doc.view())) {
    //          EOS_ASSERT( false, chain::mongo_db_insert_fail, "Failed to
    //          insert trans ${id}", ("id", t->id));
    //       }
    //    } catch(...) {
    //       handle_mongo_exception("trans_traces insert: " + json, __LINE__);
    //    }

    /// handle action trace
    for (const auto& trace : tp.trace->action_traces) {
        _handle_action_trace(trace, tp.block_num, tp.block_time);
    }
}

void mongo_db_plugin_impl::_process_accepted_block(
    const chain::block_state_ptr& bs) {
    using namespace bsoncxx::types;
    using namespace bsoncxx::builder;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    mongocxx::options::update update_opts{};
    update_opts.upsert(true);

    auto block_num = bs->block_num;
    const auto block_id = bs->id;
    const auto block_id_str = block_id.str();
    const auto prev_block_id_str = bs->block->previous.str();

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds{
            fc::time_point::now().time_since_epoch().count()});

    const chain::block_header_state& bhs = *bs;

    auto block_states = mongo_conn[db_name][block_states_col];
    auto block_state_doc = bsoncxx::builder::basic::document{};
    block_state_doc.append(
        kvp("block_num", b_int32{static_cast<int32_t>(block_num)}),
        kvp("block_id", block_id_str), kvp("validated", b_bool{bs->validated}),
        kvp("in_current_chain", b_bool{bs->in_current_chain}));

    auto json = fc::json::to_string(bhs);
    try {
        const auto& value = bsoncxx::from_json(json);
        block_state_doc.append(kvp("block_header_state", value));
    } catch (bsoncxx::exception&) {
        try {
            json = fc::prune_invalid_utf8(json);
            const auto& value = bsoncxx::from_json(json);
            block_state_doc.append(kvp("block_header_state", value));
            block_state_doc.append(kvp("non-utf8-purged", b_bool{true}));
        } catch (bsoncxx::exception& e) {
            elog(
                "Unable to convert block_header_state JSON to MongoDB JSON: "
                "${e}",
                ("e", e.what()));
            elog("  JSON: ${j}", ("j", json));
        }
    }
    block_state_doc.append(kvp("createdAt", b_date{now}));

    try {
        if (!block_states.update_one(
                make_document(kvp("block_id", block_id_str)),
                make_document(kvp("$set", block_state_doc.view())),
                update_opts)) {
            EOS_ASSERT(false, chain::mongo_db_insert_fail,
                       "Failed to insert block_state ${bid}",
                       ("bid", block_id));
        }
    } catch (...) {
        handle_mongo_exception("block_states insert: " + json, __LINE__);
    }

    auto blocks = mongo_conn[db_name][blocks_col];
    auto block_doc = bsoncxx::builder::basic::document{};
    block_doc.append(kvp("block_num", b_int32{static_cast<int32_t>(block_num)}),
                     kvp("block_id", block_id_str),
                     kvp("irreversible", b_bool{false}));

    auto v = to_variant_with_abi(*bs->block, accounts, abi_serializer_max_time);
    json = fc::json::to_string(v);
    try {
        const auto& value = bsoncxx::from_json(json);
        block_doc.append(kvp("block", value));
    } catch (bsoncxx::exception&) {
        try {
            json = fc::prune_invalid_utf8(json);
            const auto& value = bsoncxx::from_json(json);
            block_doc.append(kvp("block", value));
            block_doc.append(kvp("non-utf8-purged", b_bool{true}));
        } catch (bsoncxx::exception& e) {
            elog("Unable to convert block JSON to MongoDB JSON: ${e}",
                 ("e", e.what()));
            elog("  JSON: ${j}", ("j", json));
        }
    }
    block_doc.append(kvp("createdAt", b_date{now}));

    try {
        if (!blocks.update_one(make_document(kvp("block_id", block_id_str)),
                               make_document(kvp("$set", block_doc.view())),
                               update_opts)) {
            EOS_ASSERT(false, chain::mongo_db_insert_fail,
                       "Failed to insert block ${bid}", ("bid", block_id));
        }
    } catch (...) {
        handle_mongo_exception("blocks insert: " + json, __LINE__);
    }
}

void mongo_db_plugin_impl::_process_irreversible_block(
    const chain::block_state_ptr& bs) {
    using namespace bsoncxx::types;
    using namespace bsoncxx::builder;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    auto blocks = mongo_conn[db_name][blocks_col];  // Blocks
    auto trans = mongo_conn[db_name][trans_col];    // Transactions

    const auto block_id = bs->block->id();
    const auto block_id_str = block_id.str();
    const auto block_num = bs->block->block_num();

    // genesis block 1 is not signaled to accepted_block
    if (block_num < 2) return;

    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds{
            fc::time_point::now().time_since_epoch().count()});

    auto ir_block = find_block(blocks, block_id_str);
    if (!ir_block) {
        _process_accepted_block(bs);
        ir_block = find_block(blocks, block_id_str);
        if (!ir_block) return;  // should never happen
    }

    auto update_doc = make_document(
        kvp("$set",
            make_document(kvp("irreversible", b_bool{true}),
                          kvp("validated", b_bool{bs->validated}),
                          kvp("in_current_chain", b_bool{bs->in_current_chain}),
                          kvp("updatedAt", b_date{now}))));

    blocks.update_one(
        make_document(kvp("_id", ir_block->view()["_id"].get_oid())),
        update_doc.view());

    bool transactions_in_block = false;
    mongocxx::options::bulk_write bulk_opts;
    bulk_opts.ordered(false);
    auto bulk = trans.create_bulk_write(bulk_opts);

    for (const auto& receipt : bs->block->transactions) {
        string trx_id_str;
        if (receipt.trx.contains<packed_transaction>()) {
            const auto& pt = receipt.trx.get<packed_transaction>();
            // get id via get_raw_transaction() as packed_transaction.id()
            // mutates inernal transaction state
            const auto& raw = pt.get_raw_transaction();
            const auto& id = fc::raw::unpack<transaction>(raw).id();
            trx_id_str = id.str();
        } else {
            const auto& id = receipt.trx.get<transaction_id_type>();
            trx_id_str = id.str();
        }

        auto ir_trans = find_transaction(trans, trx_id_str);

        if (ir_trans) {
            auto update_doc = make_document(kvp(
                "$set",
                make_document(
                    kvp("irreversible", b_bool{true}),
                    kvp("block_id", block_id_str),
                    kvp("block_num", b_int32{static_cast<int32_t>(block_num)}),
                    kvp("updatedAt", b_date{now}))));

            mongocxx::model::update_one update_op{
                make_document(kvp("_id", ir_trans->view()["_id"].get_oid())),
                update_doc.view()};
            bulk.append(update_op);
            transactions_in_block = true;
        }
    }

    if (transactions_in_block) {
        try {
            if (!bulk.execute()) {
                EOS_ASSERT(false, chain::mongo_db_insert_fail,
                           "Bulk transaction insert failed for block: ${bid}",
                           ("bid", block_id));
            }
        } catch (...) {
            handle_mongo_exception("bulk transaction insert", __LINE__);
        }
    }
}

void mongo_db_plugin_impl::_handle_action_trace(const chain::action_trace& at,
                                                uint32_t block_num,
                                                fc::time_point block_time) {
    if (at.receipt.receiver == chain::config::system_account_name) {
        _handle_system_action_trace(at, block_num, block_time);
    } else if (at.receipt.receiver == N(eosio.token)) {
        _handle_eos_token_action_trace(at, block_num, block_time);
    }

    for (const auto& iline : at.inline_traces) {
        _handle_action_trace(iline, block_num, block_time);
    }
}

void mongo_db_plugin_impl::_handle_system_action_trace(
    const chain::action_trace& at, uint32_t block_num,
    fc::time_point block_time) {
    if (at.act.name == N(buyrambytes) || at.act.name == N(buyram) ||
        at.act.name == N(sellram)) {
        _handle_ramex_action_trace(at, block_num, block_time);
    } else if (at.act.name == N(setram)) {
        _handle_setram_action_trace(at);
        return;
    }
}

void mongo_db_plugin_impl::_handle_setram_action_trace(
    const chain::action_trace& at) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    uint64_t max_ram_size = 64ll * 1024 * 1024 * 1024;
    try {
        auto find = global_collection.find_one(
            make_document(kvp("key", "max_ram_size")));
        if (find) {
            auto view = find->view();
            max_ram_size =
                fc::to_int64(view["value"].get_utf8().value.to_string());
        }
    } catch (...) {
        handle_mongo_exception("get global max_ram_size", __LINE__);
    }

    const auto data = at.act.data_as<chain::setram>();
    auto delta = int64_t(data.max_ram_size) - int64_t(max_ram_size);
    rammarket.base.balance += chain::asset(delta, chain::symbol(0, "RAM"));

    update_rammarket_to_db(global_collection, rammarket);

    mongocxx::options::update update_opts{};
    update_opts.upsert(true);

    try {
        global_collection.update_one(
            make_document(kvp("key", "max_ram_size")),
            make_document(kvp(
                "$set",
                make_document(kvp("value", fc::to_string(data.max_ram_size)))
                    .view())),
            update_opts);
    } catch (...) {
        handle_mongo_exception("update global max_ram_size", __LINE__);
    }
}

void mongo_db_plugin_impl::_handle_ramex_action_trace(
    const chain::action_trace& at, uint32_t block_num,
    fc::time_point block_time) {
    using namespace bsoncxx::types;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    // auto& chain = chain_plug->chain();

    auto act_doc = bsoncxx::builder::basic::document();
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::microseconds{block_time.time_since_epoch().count()});
    act_doc.append(
        kvp("global_seq",
            b_int64{static_cast<int64_t>(at.receipt.global_sequence)}),
        kvp("block_num", b_int32{static_cast<int32_t>(block_num)}),
        kvp("block_time", b_date{ts}), kvp("trx_id", at.trx_id.str()),
        kvp("action", at.act.name.to_string()));

    auto sim_buy_ram = [&](RamMarket & market, int64_t quant) -> auto {
        auto fee = quant;
        fee = (fee + 199) / 200;  /// .5% fee (round up)
        auto quant_after_fee = quant;
        quant_after_fee -= fee;
        int64_t bytes_out =
            market
                .convert(
                    chain::asset(quant_after_fee, chain::symbol(CORE_SYMBOL)),
                    chain::symbol(0, "RAM"))
                .get_amount();
        return std::make_tuple(bytes_out, fee);
    };

    auto sim_sell_ram = [&](RamMarket & market, int64_t bytes) -> auto {
        auto tokens_out =
            market
                .convert(chain::asset(bytes, chain::symbol(0, "RAM")),
                         chain::symbol(CORE_SYMBOL))
                .get_amount();
        auto fee = (tokens_out + 199) / 200;
        return std::make_tuple(tokens_out, fee);
    };

    if (at.act.name == N(buyrambytes)) {
        const auto data = at.act.data_as<chain::buyrambytes>();
        act_doc.append(kvp("is_buy", b_bool{true}),
                       kvp("operator", data.payer.to_string()),
                       kvp("receiver", data.receiver.to_string()),
                       kvp("bytes", b_int64{static_cast<int64_t>(data.bytes)}));
        RamMarket market1 = RamMarket(rammarket);
        auto eos_out =
            market1.convert(chain::asset(static_cast<int64_t>(data.bytes),
                                         chain::symbol(0, "RAM")),
                            chain::symbol(CORE_SYMBOL));
        auto tp = sim_buy_ram(rammarket, eos_out.get_amount());
        act_doc.append(kvp("price", b_int64{eos_out.get_amount()}),
                       kvp("fee", b_int64{std::get<1>(tp)}));
    } else if (at.act.name == N(buyram)) {
        const auto data = at.act.data_as<chain::buyram>();
        act_doc.append(kvp("is_buy", b_bool{true}),
                       kvp("operator", data.payer.to_string()),
                       kvp("receiver", data.receiver.to_string()),
                       kvp("price", b_int64{data.quant.get_amount()}));
        auto tp = sim_buy_ram(rammarket, data.quant.get_amount());
        act_doc.append(kvp("bytes", b_int64{std::get<0>(tp)}),
                       kvp("fee", b_int64{std::get<1>(tp)}));
    } else if (at.act.name == N(sellram)) {
        const auto data = at.act.data_as<chain::sellram>();
        act_doc.append(kvp("is_buy", b_bool{false}),
                       kvp("operator", data.account.to_string()),
                       kvp("bytes", b_int64{data.bytes}));
        auto tp = sim_sell_ram(rammarket, data.bytes);
        act_doc.append(kvp("price", b_int64{std::get<0>(tp)}),
                       kvp("fee", b_int64{std::get<1>(tp)}));
    }

    auto ram_trade = mongo_conn[db_name][ram_trade_col];

    mongocxx::options::update update_opts{};
    update_opts.upsert(true);

    try {
        ram_trade.update_one(
            make_document(
                kvp("global_seq", fc::to_string(at.receipt.global_sequence))),
            make_document(kvp("$set", act_doc.view())), update_opts);
    } catch (...) {
        handle_mongo_exception("record ram exchange action.", __LINE__);
    }

    update_rammarket_to_db(global_collection, rammarket);
}

void mongo_db_plugin_impl::_handle_eos_token_action_trace(
    const chain::action_trace& at, uint32_t block_num,
    fc::time_point block_time) {
    using namespace bsoncxx::types;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    auto act_doc = bsoncxx::builder::basic::document();
    if (at.act.name == N(transfer)) {
        const auto data = at.act.data_as<chain::transfer>();
        // currenty only handle the EOS token
        if (data.quantity.symbol_name() != CORE_SYMBOL_NAME) return;

        accounts = mongo_conn[db_name][accounts_col];
        sync_account_balance(data.from, accounts);
        sync_account_balance(data.to, accounts);

    } else if (at.act.name == N(issue)) {
        const auto data = at.act.data_as<chain::issue>();
        // currenty only handle the EOS token
        if (data.quantity.symbol_name() != CORE_SYMBOL_NAME) return;

        accounts = mongo_conn[db_name][accounts_col];
        sync_account_balance(chain::config::system_account_name, accounts);
    }
}

mongo_db_plugin_impl::mongo_db_plugin_impl() : mongo_inst{}, mongo_conn{} {}

mongo_db_plugin_impl::~mongo_db_plugin_impl() {
    if (!startup) {
        try {
            ilog(
                "mongo_db_plugin shutdown in process please be patient this "
                "can take a few minutes");
            done = true;
            condition.notify_one();

            consume_thread.join();
        } catch (std::exception& e) {
            elog(
                "Exception on mongo_db_plugin shutdown of consume thread: ${e}",
                ("e", e.what()));
        }
    }
}

void mongo_db_plugin_impl::wipe_database() {
    ilog("mongo db wipe_database");

    auto block_states = mongo_conn[db_name][block_states_col];
    auto blocks = mongo_conn[db_name][blocks_col];
    auto trans = mongo_conn[db_name][trans_col];
    auto trans_traces = mongo_conn[db_name][trans_traces_col];
    auto actions = mongo_conn[db_name][actions_col];
    auto ram_trade = mongo_conn[db_name][ram_trade_col];
    accounts = mongo_conn[db_name][accounts_col];
    global_collection = mongo_conn[db_name][global_col];

    block_states.drop();
    blocks.drop();
    trans.drop();
    trans_traces.drop();
    actions.drop();
    accounts.drop();
    ram_trade.drop();
    global_collection.drop();
}

void mongo_db_plugin_impl::init() {
    using namespace bsoncxx::types;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    // Create the native contract accounts manually; sadly, we can't run their
    // contracts to make them create themselves See
    // native_contract_chain_initializer::prepare_database()

    accounts = mongo_conn[db_name][accounts_col];
    if (accounts.count(make_document()) == 0) {
        auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::microseconds{
                fc::time_point::now().time_since_epoch().count()});

        auto doc = make_document(
            kvp("name", name(chain::config::system_account_name).to_string()));

        try {
            if (!accounts.insert_one(doc.view())) {
                EOS_ASSERT(
                    false, chain::mongo_db_insert_fail,
                    "Failed to insert account ${n}",
                    ("n",
                     name(chain::config::system_account_name).to_string()));
            }
        } catch (...) {
            handle_mongo_exception("account insert", __LINE__);
        }

        try {
            // blocks indexes
            auto blocks = mongo_conn[db_name][blocks_col];  // Blocks
            blocks.create_index(
                bsoncxx::from_json(R"xxx({ "block_num" : 1 })xxx"));
            blocks.create_index(
                bsoncxx::from_json(R"xxx({ "block_id" : 1 })xxx"));

            auto block_stats = mongo_conn[db_name][block_states_col];
            block_stats.create_index(
                bsoncxx::from_json(R"xxx({ "block_num" : 1 })xxx"));
            block_stats.create_index(
                bsoncxx::from_json(R"xxx({ "block_id" : 1 })xxx"));

            // accounts indexes
            accounts.create_index(
                bsoncxx::from_json(R"xxx({ "name" : 1 })xxx"));

            // transactions indexes
            auto trans = mongo_conn[db_name][trans_col];  // Transactions
            trans.create_index(bsoncxx::from_json(R"xxx({ "trx_id" : 1 })xxx"));

            auto actions = mongo_conn[db_name][actions_col];
            actions.create_index(
                bsoncxx::from_json(R"xxx({ "trx_id" : 1 })xxx"));

            auto ram_trade = mongo_conn[db_name][ram_trade_col];
            ram_trade.create_index(
                bsoncxx::from_json(R"xxx({ "global_seq" : 1 })xxx"));
        } catch (...) {
            handle_mongo_exception("create indexes", __LINE__);
        }
    }

    global_collection = mongo_conn[db_name][global_col];
    rammarket = get_rammarket_from_db(global_collection);

    ilog("starting db plugin thread");

    consume_thread = boost::thread([this] { consume_blocks(); });

    startup = false;
}

////////////
// mongo_db_plugin
////////////

mongo_db_plugin::mongo_db_plugin() : my(new mongo_db_plugin_impl) {}

mongo_db_plugin::~mongo_db_plugin() {}

void mongo_db_plugin::set_program_options(options_description& cli,
                                          options_description& cfg) {
    cfg.add_options()(
        "mongodb-queue-size,q", bpo::value<uint32_t>()->default_value(256),
        "The target queue size between nodeos and MongoDB plugin thread.")(
        "mongodb-wipe", bpo::bool_switch()->default_value(false),
        "Required with --replay-blockchain, --hard-replay-blockchain, or "
        "--delete-all-blocks to wipe mongo db."
        "This option required to prevent accidental wipe of mongo db.")(
        "mongodb-block-start", bpo::value<uint32_t>()->default_value(0),
        "If specified then only abi data pushed to mongodb until specified "
        "block is reached.")(
        "mongodb-uri,m", bpo::value<std::string>(),
        "MongoDB URI connection string, see: "
        "https://docs.mongodb.com/master/reference/connection-string/."
        " If not specified then plugin is disabled. Default database 'EOS' is "
        "used if not specified in URI."
        " Example: mongodb://127.0.0.1:27017/EOS");
}

void mongo_db_plugin::plugin_initialize(const variables_map& options) {
    try {
        if (options.count("mongodb-uri")) {
            ilog("initializing mongo_db_plugin");
            my->configured = true;

            if (options.at("replay-blockchain").as<bool>() ||
                options.at("hard-replay-blockchain").as<bool>() ||
                options.at("delete-all-blocks").as<bool>()) {
                if (options.at("mongodb-wipe").as<bool>()) {
                    ilog("Wiping mongo database on startup");
                    my->wipe_database_on_startup = true;
                } else if (options.count("mongodb-block-start") == 0) {
                    EOS_ASSERT(
                        false, chain::plugin_config_exception,
                        "--mongodb-wipe required with --replay-blockchain, "
                        "--hard-replay-blockchain, or --delete-all-blocks"
                        " --mongodb-wipe will remove all EOS collections from "
                        "mongodb.");
                }
            }

            if (options.count("abi-serializer-max-time-ms") == 0) {
                EOS_ASSERT(false, chain::plugin_config_exception,
                           "--abi-serializer-max-time-ms required as default "
                           "value not appropriate for parsing full blocks");
            }
            my->abi_serializer_max_time =
                app().get_plugin<chain_plugin>().get_abi_serializer_max_time();

            if (options.count("mongodb-queue-size")) {
                my->queue_size =
                    options.at("mongodb-queue-size").as<uint32_t>();
            }
            if (options.count("mongodb-block-start")) {
                my->start_block_num =
                    options.at("mongodb-block-start").as<uint32_t>();
            }
            if (my->start_block_num == 0) {
                my->start_block_reached = true;
            }

            std::string uri_str = options.at("mongodb-uri").as<std::string>();
            ilog("connecting to ${u}", ("u", uri_str));
            mongocxx::uri uri = mongocxx::uri{uri_str};
            my->db_name = uri.database();
            if (my->db_name.empty()) my->db_name = "EOS";
            my->mongo_conn = mongocxx::client{uri};

            // hook up to signals on controller
            chain_plugin* chain_plug = app().find_plugin<chain_plugin>();
            EOS_ASSERT(chain_plug, chain::missing_chain_plugin_exception, "");
            auto& chain = chain_plug->chain();
            my->chain_plug = chain_plug;
            my->chain_id.emplace(chain.get_chain_id());

            // TODO: ignore
            //    my->accepted_block_connection.emplace(
            //    chain.accepted_block.connect( [&]( const
            //    chain::block_state_ptr& bs ) {
            //       my->accepted_block( bs );
            //    } ));
            //    my->irreversible_block_connection.emplace(
            //          chain.irreversible_block.connect( [&]( const
            //          chain::block_state_ptr& bs ) {
            //             my->applied_irreversible_block( bs );
            //          } ));
            my->accepted_transaction_connection.emplace(
                chain.accepted_transaction.connect(
                    [&](const chain::transaction_metadata_ptr& t) {
                        my->accepted_transaction(t);
                    }));
            my->applied_transaction_connection.emplace(
                chain.applied_transaction.connect(
                    [&](const chain::transaction_trace_ptr& t) {
                        auto& chain = my->chain_plug->chain();
                        const auto& tp =
                            mongo_db_plugin_impl::action_trace_tuple{
                                t, chain.pending_block_state()->block_num,
                                chain.pending_block_time()};
                        my->applied_transaction(tp);
                    }));

            if (my->wipe_database_on_startup) {
                my->wipe_database();
            }
            my->init();
        } else {
            wlog(
                "eosio::mongo_db_plugin configured, but no --mongodb-uri "
                "specified.");
            wlog("mongo_db_plugin disabled.");
        }
    }
    FC_LOG_AND_RETHROW()
}

void mongo_db_plugin::plugin_startup() {}

void mongo_db_plugin::plugin_shutdown() {
    my->accepted_block_connection.reset();
    my->irreversible_block_connection.reset();
    my->accepted_transaction_connection.reset();
    my->applied_transaction_connection.reset();

    my.reset();
}

}  // namespace eosio
