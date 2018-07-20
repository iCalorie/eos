
#include <eosio/mongo_db_plugin/ram_market.hpp>

#include <eosio/chain/symbol.hpp>
#include <eosio/chain/config.hpp>
#include <eosio/chain/exceptions.hpp>


namespace eosio {

using chain::asset;
using chain::symbol;

RamMarket::RamMarket(){
    supply = asset(100000000000000ll,symbol(4,"RAMCORE"));
    base.balance = asset(int64_t(64ll * 1024 * 1024 * 1024),symbol(0,"RAM"));
    quote.balance = asset(10000000000ll,symbol(CORE_SYMBOL));
}

RamMarket::RamMarket(const fc::variant &v) {
  supply = asset::from_string(v["supply"].as_string());
  base.balance = asset::from_string(v["base"]["balance"].as_string());
  quote.balance = asset::from_string(v["quote"]["balance"].as_string());
  base.weight = v["base"]["weight"].as_double();
  quote.weight = v["quote"]["weight"].as_double();
}

RamMarket::RamMarket(const RamMarket &r) {
  supply = asset(r.supply.get_amount(), r.supply.get_symbol());
  base = RamMarket::connector{
      asset(r.base.balance.get_amount(), r.base.balance.get_symbol()),
      r.base.weight};
  quote = RamMarket::connector{
      asset(r.quote.balance.get_amount(), r.quote.balance.get_symbol()),
      r.quote.weight};
}

asset RamMarket::convert_to_exchange(connector &c, asset in) {
  real_type R(supply.get_amount());
  real_type C(c.balance.get_amount() + in.get_amount());
  real_type F(c.weight / 1000.0);
  real_type T(in.get_amount());
  real_type ONE(1.0);

  real_type E = -R * (ONE - std::pow(ONE + T / C, F));
  // print( "E: ", E, "\n");
  int64_t issued = int64_t(E);

  supply += asset(issued, supply.get_symbol());
  c.balance += in;

  return asset(issued, supply.get_symbol());
}

asset RamMarket::convert_from_exchange(connector &c, asset in) {
  real_type R(supply.get_amount() - in.get_amount());
  real_type C(c.balance.get_amount());
  real_type F(1000.0 / c.weight);
  real_type E(in.get_amount());
  real_type ONE(1.0);

  // potentially more accurate:
  // The functions std::expm1 and std::log1p are useful for financial
  // calculations, for example, when calculating small daily interest rates:
  // (1+x)n -1 can be expressed as std::expm1(n * std::log1p(x)). real_type T =
  // C * std::expm1( F * std::log1p(E/R) );

  real_type T = C * (std::pow(ONE + E / R, F) - ONE);
  // print( "T: ", T, "\n");
  int64_t out = int64_t(T);

  supply -= in;
  auto outAsset = asset(out, c.balance.get_symbol());
  c.balance -= outAsset;

  return outAsset;
}

asset RamMarket::convert(asset from, symbol to) {
  auto sell_symbol = from.get_symbol();
  auto ex_symbol = supply.get_symbol();
  auto base_symbol = base.balance.get_symbol();
  auto quote_symbol = quote.balance.get_symbol();

  if (sell_symbol != ex_symbol) {
    if (sell_symbol == base_symbol) {
      from = convert_to_exchange(base, from);
    } else if (sell_symbol == quote_symbol) {
      from = convert_to_exchange(quote, from);
    } else {
      FC_ASSERT(false, "invalid sell");
    }
  } else {
    if (to == base_symbol) {
      from = convert_from_exchange(base, from);
    } else if (to == quote_symbol) {
      from = convert_from_exchange(quote, from);
    } else {
      FC_ASSERT(false, "invalid conversion");
    }
  }

  if (to != from.get_symbol()) return convert(from, to);

  return from;
}
}  // namespace eosio