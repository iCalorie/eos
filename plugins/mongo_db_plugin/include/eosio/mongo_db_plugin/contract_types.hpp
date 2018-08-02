#pragma once

#include <eosio/chain/authority.hpp>
#include <eosio/chain/chain_config.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/types.hpp>

namespace eosio {
namespace chain {

using action_name = eosio::chain::action_name;

struct setram {
    uint64_t max_ram_size;

    static account_name get_account() { return config::system_account_name; }

    static action_name get_name() { return N(setram); }
};

struct buyrambytes {
    account_name payer;
    account_name receiver;
    uint32_t bytes;

    static account_name get_account() { return config::system_account_name; }

    static action_name get_name() { return N(buyrambytes); }
};

struct buyram {
    account_name payer;
    account_name receiver;
    asset quant;

    static account_name get_account() { return config::system_account_name; }

    static action_name get_name() { return N(buyram); }
};

struct sellram {
    account_name account;
    int64_t bytes;

    static account_name get_account() { return config::system_account_name; }

    static action_name get_name() { return N(sellram); }
};

struct issue {
    account_name to;
    asset quantity;
    string memo;

    static account_name get_account() { return N(eosio.token); }

    static action_name get_name() { return N(issue); }
};

struct transfer {
    account_name from;
    account_name to;
    asset quantity;
    string memo;

    static account_name get_account() { return N(eosio.token); }

    static action_name get_name() { return N(transfer); }
};

struct delegated_bandwidth {
    account_name from;
    account_name to;
    asset net_weight;
    asset cpu_weight;
};

struct refund_request {
    account_name owner;
    uint32_t request_time;
    asset net_amount;
    asset cpu_amount;
};

}  // namespace chain
}  // namespace eosio

FC_REFLECT(eosio::chain::setram, (max_ram_size))
FC_REFLECT(eosio::chain::buyrambytes, (payer)(receiver)(bytes))
FC_REFLECT(eosio::chain::buyram, (payer)(receiver)(quant))
FC_REFLECT(eosio::chain::sellram, (account)(bytes))

FC_REFLECT(eosio::chain::issue, (to)(quantity)(memo))
FC_REFLECT(eosio::chain::transfer, (from)(to)(quantity)(memo))

FC_REFLECT(eosio::chain::delegated_bandwidth, (from)(to)(net_weight)(cpu_weight))
FC_REFLECT(eosio::chain::refund_request, (owner)(request_time)(net_amount)(cpu_amount))
