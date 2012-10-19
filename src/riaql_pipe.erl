-module(riaql_pipe).
-export([process/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

process(Query) ->
    PipeSpec = [#fitting_spec{name=fetch,
                   module=riak_kv_pipe_get,
                   chashfun=follow,
                   nval={riak_kv_pipe_get, bkey_nval}},
                #fitting_spec{name=select,
                   module = riak_pipe_w_xform,
                   arg=fun(Input, Partition, FittingDetails) ->
                           Results = map(Input, FittingDetails, Query),
                           send_results(Results, Partition, FittingDetails)
                       end,
                   chashfun=follow,
                   q_limit = 64,
                   nval=1}],
    {ok, Pipe} = riak_pipe:exec(PipeSpec, []),%[{trace, all},{log, lager}])
    case From = proplists:get_value(from, Query) of
        {index, Bucket, Index, Match} ->
            riak_kv_pipe_index:queue_existing_pipe(Pipe, Bucket, {eq, Index, Match}, 60000);
        {index, Bucket, Index, Start, End} ->
            riak_kv_pipe_index:queue_existing_pipe(Pipe, Bucket, {range, Index, Start, End}, 60000);
        Bucket when is_binary(Bucket) ->
            riak_kv_pipe_listkeys:queue_existing_pipe(Pipe, From, 60000)
    end,
    {_, Results, _} = riak_pipe:collect_results(Pipe),
    Results.

send_results([], _, _) ->
    ok;
send_results([Result|Results], P, FD)  ->
    case riak_pipe_vnode_worker:send_output(Result, P, FD) of
        ok ->
            send_results(Results, P, FD);
        ER ->
            throw(ER)
    end.

map(Input,_,Args) ->
    map_key_value(erlang_input(Input), none, Args).

map_key_value({error, notfound}, _, _) ->
    [];
map_key_value(RObj, _KD, Query) ->
    case (catch mochijson2:decode(riak_object:get_value(RObj))) of
        {'EXIT', _} ->
            lager:info("Dail");
        DJson={struct, _} ->
            Select = proplists:get_value(select, Query),
            case proplists:get_value(where, Query) of
                Where=[{_,_}|_] ->
                    case lists:all(fun({Key, Pred}) ->
                                       where_key(Key, Pred, DJson)
                                   end, Where) of
                        false -> [];
                        true -> make_key_value(RObj, DJson, Select)
                    end;
                _ ->
                    make_key_value(RObj, DJson, Select)
            end;
        _ ->
            []
    end.

make_key_value(RObj, DJson, Select) ->
    [{
      <<
        (riak_object:bucket(RObj))/bitstring,
        <<"/">>/bitstring,
        (riak_object:key(RObj))/bitstring
      >>,
      select_keys(
                  Select,
                  DJson
                 )
     }].

where_key(Key, Pred, {struct, PList}) ->
    case lists:keyfind(Key, 1, PList) of
        false -> false;
        {Key, Value} -> Pred(Value)
    end.

select_keys(<<"*">>, DJson) ->
    DJson;
select_keys(Keys=[{_,_}|_], {struct, PList}) ->
    {struct, lists:foldl(fun({Key, Val}, Acc) ->
        case lists:keyfind(Key, 2, Keys) of
            false -> Acc;
            {NewKey, Key} -> [{NewKey, Val}|Acc]
        end
    end, [], PList)}.

bucket(Bucket) ->
    Bucket.

index(Bucket, Index, Key) ->
    {index, Bucket, Index, Key}.
index(Bucket, Index, StartKey, EndKey) ->
    {index, Bucket, Index, StartKey, EndKey}.

erlang_input({ok, Input, _})          -> Input;
erlang_input({{error,_}=Input, _, _}) -> Input.
