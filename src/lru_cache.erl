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

-record(cache, {capacity, num_entries=0, seq=0, values, order}).

new(Capacity) when Capacity >= 1 ->
    #cache{capacity=Capacity, values=dict:new(), order=gb_trees:empty()}.

flush(Cache) ->
    new(Cache#cache.capacity).

get(Key, Cache) ->
    case dict:find(Key, Cache#cache.values) of
        {ok, {Value, Seq}} ->
            {Value, update_lru(Key, Seq, Value, Cache)};
        error ->
            false
    end.

put(Key, Value, Cache) ->
    Seq = Cache#cache.seq,
    Cache1 = Cache#cache{values=dict:store(Key, {Value, Seq}, Cache#cache.values),
                         seq=Seq + 1},
    case dict:find(Key, Cache#cache.values) of
        {_, OldSeq} ->
            Cache1#cache{order=gb_trees:enter(Seq, Key, gb_trees:delete(OldSeq, Cache#cache.order))};
        error ->
            add_key(Key, Seq, Cache1)
    end.

add_key(Key, Seq, Cache) ->
    Cache1 = Cache#cache{order=gb_trees:enter(Seq, Key, Cache#cache.order)},
    case Cache1#cache.num_entries of
        Capacity when Capacity =:= Cache1#cache.capacity ->
            remove_lru(Cache1);
        NEntries ->
            Cache1#cache{num_entries=NEntries + 1}
    end.

update_lru(_, OldSeq, _, Cache) when Cache#cache.seq =:= OldSeq + 1 ->
    Cache;
update_lru(Key, OldSeq, Value, Cache) ->
    Seq = Cache#cache.seq,
    Cache#cache{seq=Seq + 1,
                order=gb_trees:enter(Seq, Key, gb_trees:delete(OldSeq, Cache#cache.order)),
                values=dict:store(Key, {Value, Seq}, Cache#cache.values)}.

remove_lru(Cache) ->
    {_, Key, NewOrder} = gb_trees:take_smallest(Cache#cache.order),
    Cache#cache{order=NewOrder, values=dict:erase(Key, Cache#cache.values)}.
