%%%-------------------------------------------------------------------
%%% File:      lru_cache.erl
%%% @author    Brian Buchanan <bwb@holo.org>
%%% @copyright 2008-2009 Brian Buchanan
%%% @doc  
%%% This module implements a variable-sized key/value cache with
%%% a least-recently-used purge strategy.
%%% @end  
%%%
%%%-------------------------------------------------------------------
-module(lru_cache).
-author('bwb@holo.org').
-export([new/1, get/2, put/3, flush/1]).

-record(lru_cache, {capacity, num_entries=0, seq=0, values, order}).

new(Capacity) when Capacity >= 1 ->
    #lru_cache{capacity=Capacity, values=dict:new(), order=gb_trees:empty()}.

flush(Cache) ->
    new(Cache#lru_cache.capacity).

get(Key, Cache) ->
    case dict:find(Key, Cache#lru_cache.values) of
        {ok, {Value, Seq}} ->
            {Value, update_lru(Key, Seq, Value, Cache)};
        error ->
            false
    end.

put(Key, Value, Cache) ->
    Seq = Cache#lru_cache.seq,
    Cache1 = Cache#lru_cache{values=dict:store(Key, {Value, Seq}, Cache#lru_cache.values),
                             seq=Seq + 1},
    case dict:find(Key, Cache#lru_cache.values) of
        {_, OldSeq} ->
            Cache1#lru_cache{order=gb_trees:enter(Seq, Key, gb_trees:delete(OldSeq, Cache#lru_cache.order))};
        error ->
            add_key(Key, Seq, Cache1)
    end.

add_key(Key, Seq, Cache) ->
    Cache1 = Cache#lru_cache{order=gb_trees:enter(Seq, Key, Cache#lru_cache.order)},
    case Cache1#lru_cache.num_entries of
        Capacity when Capacity =:= Cache1#lru_cache.capacity ->
            remove_lru(Cache1);
        NEntries ->
            Cache1#lru_cache{num_entries=NEntries + 1}
    end.

update_lru(_, OldSeq, _, Cache) when Cache#lru_cache.seq =:= OldSeq + 1 ->
    Cache;
update_lru(Key, OldSeq, Value, Cache) ->
    Seq = Cache#lru_cache.seq,
    Cache#lru_cache{seq=Seq + 1,
                    order=gb_trees:enter(Seq, Key, gb_trees:delete(OldSeq, Cache#lru_cache.order)),
                    values=dict:store(Key, {Value, Seq}, Cache#lru_cache.values)}.

remove_lru(Cache) ->
    {_, Key, NewOrder} = gb_trees:take_smallest(Cache#lru_cache.order),
    Cache#lru_cache{order=NewOrder, values=dict:erase(Key, Cache#lru_cache.values)}.
