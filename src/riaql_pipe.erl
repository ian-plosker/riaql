-module(riaql_pipe).
-export([process/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

process(Query) ->
    Select = proplists:get_value(select, Query),

    PipeSpec = [#fitting_spec{name=fetch,
                   module=riak_kv_pipe_get,
                   chashfun=follow,
                   nval={riak_kv_pipe_get, bkey_nval}},
                #fitting_spec{name=decode,
                   module=riak_pipe_w_xform,
                   chashfun=follow,
                   arg=fun(Input, Partition, FittingDetails) ->
                           Results = make_key_value(erlang_input(Input)),
                           send_results(Results, Partition, FittingDetails)
                   end,
                   q_limit=100,
                   nval = 1}
               ] ++
            case proplists:get_value(where, Query) of
                Where=[{_,_}|_] ->
                    [#fitting_spec{name=where,
                        module = riak_pipe_w_xform,
                        arg=fun(Input, Partition, FittingDetails) ->
                           Results = filter(Input, FittingDetails, Where),
                           send_results(Results, Partition, FittingDetails)
                        end,
                        chashfun=follow,
                        q_limit=100,
                        nval=1}];
                _ ->
                    []
            end
            ++ case proplists:get_value(select, Query) of
                <<"*">> ->
                    [];
                Select ->
                    [#fitting_spec{name=select,
                        module = riak_pipe_w_xform,
                        arg=fun(Input, Partition, FittingDetails) ->
                           Results = map(Input, Select),
                           send_results(Results, Partition, FittingDetails)
                        end,
                        chashfun=follow,
                        q_limit=100,
                        nval=1}]
            end,
    {ok, Pipe} = riak_pipe:exec(PipeSpec, []),%[{trace, all},{log, lager}])
    case proplists:get_value(from, Query) of
        {index, Bucket, Index, Match} ->
            riak_kv_pipe_index:queue_existing_pipe(
                                                   Pipe, Bucket,
                                                   {eq, Index, Match},
                                                   60000
                                                  );
        {index, Bucket, Index, Start, End} ->
            riak_kv_pipe_index:queue_existing_pipe(
                                                   Pipe, Bucket,
                                                   {range, Index, Start, End},
                                                   60000
                                                  );
        From when is_binary(From) ->
            riak_kv_pipe_listkeys:queue_existing_pipe(Pipe, From, 60000)
    end,
    {_, Results, _} = riak_pipe:collect_results(Pipe),
    [Result || {_, Result} <- Results].

send_results([], _, _) ->
    ok;
send_results([Result|Results], P, FD)  ->
    case riak_pipe_vnode_worker:send_output(Result, P, FD) of
        ok ->
            send_results(Results, P, FD);
        ER ->
            throw(ER)
    end.

map({Key,Value}, Select) ->
    [{Key, select_keys(Select, Value)}].

filter({_Key,Value}=Input, _, Where) ->
    case lists:all(fun({Key, Pred}) ->
                    where_key(Key, Pred, Value)
            end, Where) of
        false -> [];
        true -> [Input]
    end.

make_key_value({error, notfound}) ->
    [];
make_key_value(RObj) ->
    case (catch mochijson2:decode(riak_object:get_value(RObj))) of
        {'EXIT', _} ->
            [];
        DJson={struct, _} ->
            [{
              <<
                (riak_object:bucket(RObj))/bitstring,
                <<"/">>/bitstring,
                (riak_object:key(RObj))/bitstring
              >>,
              DJson
            }];
        _ ->
            []
    end.

where_key(Key, Pred, {struct, PList}) ->
    case lists:keyfind(Key, 1, PList) of
        false -> false;
        {Key, Value} -> Pred(Value)
    end.

select_keys(Keys=[{_,_}|_], {struct, PList}) ->
    {struct, lists:foldl(fun({Key, Val}, Acc) ->
        case lists:keyfind(Key, 2, Keys) of
            false -> Acc;
            {NewKey, Key} -> [{NewKey, Val}|Acc]
        end
    end, [], PList)}.

erlang_input({ok, Input, _}) ->
    Input;
erlang_input({{error,_}=Input, _, _}) ->
    Input.
