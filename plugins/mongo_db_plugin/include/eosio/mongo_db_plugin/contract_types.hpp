#pragma once

#include <eosio/chain/authority.hpp>
#include <eosio/chain/chain_config.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/types.hpp>

namespace eosio { namespace chain {

using action_name    = eosio::chain::action_name;

struct setram {
   uint64_t         max_ram_size;

   static account_name get_account() {
      return config::system_account_name;
   }

   static action_name get_name() {
      return N(setram);
   }
};

struct buyrambytes {
    account_name payer;
    account_name receiver;
    uint32_t bytes;

    static account_name get_account() {
      return config::system_account_name;
    }

   static action_name get_name() {
      return N(buyrambytes);
   }
};

struct buyram {
    account_name payer;
    account_name receiver;
    asset quant;

    static account_name get_account() {
      return config::system_account_name;
    }

   static action_name get_name() {
      return N(buyram);
   }
};

struct sellram {
    account_name account;
    int64_t bytes;

    static account_name get_account() {
      return config::system_account_name;
    }

   static action_name get_name() {
      return N(sellram);
   }
};


} } /// namespace eosio::chain

FC_REFLECT( eosio::chain::setram                       , (max_ram_size))
FC_REFLECT( eosio::chain::buyrambytes                  , (payer)(receiver)(bytes))
FC_REFLECT( eosio::chain::buyram                       , (payer)(receiver)(quant))
FC_REFLECT( eosio::chain::sellram                      , (account)(bytes))
