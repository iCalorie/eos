#include <eosiolib/asset.hpp>
#include <eosiolib/currency.hpp>
#include <eosiolib/eosio.hpp>
#include <eosiolib/singleton.hpp>

using namespace eosio;

namespace fomoFast {

#define F3D_SYMBOL S(10, F3D)

// round timer start at this
const static uint64_t ROUND_INIT_TIME = 5 * 60 * 1000;
// every full key purchased adds this much to the timer
const static uint64_t ROUND_INC_TIME = 5 * 60 * 1000;
// max length a round timer can be
const static uint64_t ROUND_MAX_TIME = 5 * 60 * 1000;
// length of ICO phase
const static uint64_t ROUND_ICO_GAP_TIME = 60 * 1000;

const static int WHALES_TEAM = 0;
const static int BEARS_TEAM = 1;
const static int SNEKS_TEAM = 2;
const static int BULLS_TEAM = 3;

class fomoFast_contract : public eosio::contract {
   private:
    static team_fee team_fees[4];    // Team allocation percentages
    static pot_split pot_splits[4];  // how to split up the final pot based on
                                     // which team was picked
    global_state_singleton _gs_singleton;
    round_table _rounds;
    player_table _players;
    round_table::const_iterator _curr_round_itr;

    global_state _gstate;  // current game state

   public:
    fomoFast_contract(account_name self)
        : contract(self),
          _gs_singleton(_self, _self),
          _rounds(_self, _self),
          _players(_self, _self) {
        if (!_gs_singleton.exists()) {
            _gstate.activated = false;
            _gs_singleton.set(_gstate, _self);
        } else {
            _gstate = _gs_singleton.get();
            _curr_round_itr = _rounds.find(_gstate.curr_round_num);
        }

        _init_contract();
    }

    void apply(account_name contract, account_name act);

    //------------------ abi of contract -----------------------------

    /**
     * upon contract deploy, it will be deactivated.this is a one time
     * use function that will activate the contract.we do this so devs
     * have time to set things up on the web end.
     *
     * @abi
     **/
    void active();

    //----------------- end abi of contract --------------------------

   private:
    void on(const currency::transfer& t, account_name code);

    // the core method of buy key
    void buy_core(account_name player, account_name affiliate, uint32_t team_id,
                  asset eos);
    // ends the round. manages paying out winner/splitting up pot
    void end_round();
    // decides if round end needs to be run & new round started.  and if player
    // unmasked earnings from previously played rounds need to be moved.
    void manage_round_data();
    // current round info
    const round& curr_round() const { return *_curr_round_itr; }

    void _init_contract() {
        team_fees[WHALES_TEAM] = team_fee{
            53, 33, 10, 1, 3};  // 53% to pot, 33% to general, 10% to aff, 1% to
                                // air drop pot, 3% to dev_team
        team_fees[BEARS_TEAM] = team_fee{
            43, 43, 10, 1, 3};  // 43% to pot, 43% to general, 10% to aff, 1% to
                                // air drop pot, 3% to dev_team
        team_fees[SNEKS_TEAM] = team_fee{
            25, 61, 10, 1, 3};  // 25% to pot, 61% to general, 10% to aff, 1% to
                                // air drop pot, 3% to dev_team
        team_fees[BULLS_TEAM] = team_fee{
            39, 47, 10, 1, 3};  // 39% to pot, 47% to general, 10% to aff, 1% to
                                // air drop pot, 3% to dev_team

        pot_splits[WHALES_TEAM] =
            pot_split{25, 48, 25, 2};  // 25% to general, 48% to winner, 25% to
                                       // next round, 2% to dev_team
        pot_splits[BEARS_TEAM] =
            pot_split{25, 48, 25, 2};  // 25% to general, 48% to winner, 25% to
                                       // next round, 2% to dev_team
        pot_splits[SNEKS_TEAM] =
            pot_split{40, 48, 25, 2};  // 40% to general, 48% to winner, 25% to
                                       // next round, 2% to dev_team
        pot_splits[BULLS_TEAM] =
            pot_split{40, 48, 25, 2};  // 40% to general, 48% to winner, 25% to
                                       // next round, 2% to dev_team
    };

    void require_admin_auth() {
        const static account_name ADMIN_ACCOUNT1 = N(chachiloveos);
        eosio_assert(has_auth(_self) || has_auth(ADMIN_ACCOUNT1),
                     "require admin account auth!");
    }

    void require_activate() {
        eosio_assert(_gstate.activated, "Game not ready yet.");
    }
};

struct global_state {
    uint64_t curr_round_num;  // current round number;
    bool activated;  // the contract has been activated; can only be run once.

    EOSLIB_SERIALIZE(global_state, (curr_round_num)(activated))
};

//@abi play round table
struct round {
    uint64_t num;              // round number
    account_name last_player;  // player in lead
    uint8_t team;              // team id in lead
    uint64_t end_time;         // end time of this round
    bool ended;                // has round end
    uint64_t start_time;       // round start time
    int64_t keys;              //  keys
    int64_t eos;               //  total eos in
    int64_t pot;  //  eos to pot (during round) / final amount paid to winner
                  //  (after round ends)
    int64_t ico;  // total eos sent in during ICO phase
    int64_t ico_gen;  // total eos for gen during ICO phase
    int64_t ico_avg;  // average key price for ICO phase

    auto primary_key() const { return num; }
    EOSLIB_SERIALIZE(round,
                     (num)(last_player)(team)(end_time)(ended)(start_time)(
                         keys)(eos)(pot)(ico)(ico_gen)(ico_avg))
};

//@abi player record of each time
struct player_round_record {
    uint64_t r_num;  // round number
    int64_t eos;     // eos that player has add to this round;
    int64_t keys;    // keys
    int64_t ico;     // ICO phase investment

    EOSLIB_SERIALIZE(player_round_record, (r_num)(eos)(keys)(ico))
};

//@abi player table
struct player {
    account_name account;                      // player account
    int64_t win;                               // winnings eos in game
    int64_t gen_earn;                          // general earnings eos
    int64_t aff_earn;                          // affiliate earnings eos
    uint32_t last_round;                       // id of last round played
    account_name affiliate;                    // last affiliate account
    std::vector<player_round_record> records;  // player round records

    auto primary_key() const { return account; }
    EOSLIB_SERIALIZE(player,
                     (account)(win)(gen)(aff)(last_round)(affiliate)(records))
};

struct team_fee {
    int32_t pot;
    int32_t gen;
    int32_t aff;
    int32_t airdrop;
    int32_t dev;
};

struct pot_split {
    int32_t gen;
    int32_t winner;
    int32_t next_round;
    int32_t dev;
};

typedef eosio::multi_index<N(player), player> player_table;
typedef eosio::multi_index<N(round), round> round_table;
typedef eosio::singleton<N(global), global_state> global_state_singleton;
}  // namespace fomoFast
