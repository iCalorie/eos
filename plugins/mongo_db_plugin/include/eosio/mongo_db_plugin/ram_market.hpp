#include <appbase/application.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <eosio/chain/types.hpp>
#include <eosio/chain/asset.hpp>
#include <eosio/chain/action.hpp>

namespace fc {
class variant;
}

typedef double real_type;

namespace eosio {
class RamMarket {
 public:
 chain::asset supply;

  struct connector {
    chain::asset balance;
    double weight = .5;
  };

  connector base;
  connector quote;

  RamMarket();
  RamMarket(fc::variant &);
  RamMarket(const RamMarket &);

  const chain::symbol &primary_key() const { return supply.get_symbol(); }

  chain::asset convert_to_exchange(connector &c, chain::asset in);
  chain::asset convert_from_exchange(connector &c, chain::asset in);
  chain::asset convert(chain::asset from, chain::symbol &to);

  enum RAM_EX_ACT {
    RAM_EX_SELL_BYTES = -1,
    RAM_EX_INVALID = 0,
    RAM_EX_BUY = 1,
    RAM_EX_BUY_BYTES = 2
  };

  static RAM_EX_ACT is_ram_exchange_action(const chain::action &act);

};

}  // namespace eosio