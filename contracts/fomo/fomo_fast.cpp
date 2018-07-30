
#include "fomo_fast.hpp"

namespace fomoFast {

void fomoFast_contract::active() {
    require_admin_auth();  // only admin could active the game

    // activate only once
    eosio_assert(!_gstate.activated, "fomoFast already activated.");

    // activate game. and set current round number to 1
    _gstate.activated = true;
    _gstate.curr_round_num = 1;
    _gs_singleton.set(_gstate, _self);

    uint32_t _now = current_time();
    // 1st round game
    _curr_round_itr = _rounds.emplace(_self, [&](round& r) {
        r.num = 1;
        r.start_time = _now;  // start immediately;
        r.end_time = _now + ROUND_INIT_TIME + ROUND_ICO_GAP_TIME;
    });
}

void fomoFast_contract::on(const currency::transfer& t, account_name code) {
    // TODO: handle user transfer
}

void fomoFast_contract::buy_core(account_name player, account_name affiliate,
                                 uint32_t team_id, asset eos) {
    auto r_id = _gstate.curr_round_num;
    uint32_t _now = current_time();
    auto r_itr = _rounds.find(r_id);  // current round info

    // if round is active
    if (_now > r_itr->start_time &&
        (_now <= r_itr->end_time ||
         (_now > r_itr->end_time && r_itr->last_player == 0))) {
        // TODO: core
    } else {
        // check to see if end round needs to be ran
        if (_now > r_itr->end_time && !r_itr->ended) {
            end_round();
        }
    }
}

void fomoFast_contract::manage_round_data() {}

void fomoFast_contract::end_round() {
    auto r_id = _gstate.curr_round_num;
    uint32_t _now = now();
    auto r_itr = _rounds.find(r_id);

    if (_now <= r_itr->end_time || r_itr->ended) {
        return;
    }

    // TODO: 结算
    _rounds.modify(r_itr, _self, [&](round& r) { r.ended = true; });
}

void fomoFast_contract::apply(account_name contract, account_name act) {
    if (act == N(transfer)) {
        on(unpack_action_data<currency::transfer>(), contract);
        return;
    }

    if (contract != _self) {
        return;
    }

    auto& thiscontract = *this;
    switch (act) { EOSIO_API(fomoFast_contract, (active)) };
}

}  // namespace fomoFast
extern "C" {
[[noreturn]] void apply(uint64_t receiver, uint64_t code, uint64_t action) {
    fomoFast::fomoFast_contract contract(receiver);
    contract.apply(code, action);
    eosio_exit(0);
}
}