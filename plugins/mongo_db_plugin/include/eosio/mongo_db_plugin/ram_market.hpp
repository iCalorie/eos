#include <appbase/application.hpp>

#include <fc/io/json.hpp>
#include <fc/utf8.hpp>
#include <fc/variant.hpp>

#include <eosio/chain/types.hpp>
#include <eosio/chain/asset.hpp>

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
  RamMarket(const fc::variant &);
  RamMarket(const RamMarket &);

  const chain::symbol &primary_key() const { return supply.get_symbol(); }

  chain::asset convert_to_exchange(connector &c, chain::asset in);
  chain::asset convert_from_exchange(connector &c, chain::asset in);
  chain::asset convert(chain::asset from, chain::symbol to);

};

}  // namespace eosio