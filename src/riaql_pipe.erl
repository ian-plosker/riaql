-module(riaql_pipe).
-export([process/1]).

-include_lib("riak_pipe/include/riak_pipe.hrl").
-include_lib("riak_pipe/include/riak_pipe_log.hrl").

process(Query) ->
    Select = proplists:get_value(select, Query),
    From = proplists:get_value(from, Query),

    PipeSpec = lists:append([[
                #fitting_spec{name=fetch,
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
               ],
               where_fitting(proplists:get_value(where, Query)),
               select_fitting(Select),
               order_by_fittings(proplists:get_value(order_by, Query), From)]),

    {ok, Pipe} = riak_pipe:exec(PipeSpec, []),%[{trace, all},{log, lager}])
    case From of
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

where_fitting(Where={_,_,_}) ->
    [#fitting_spec{name=where,
        module = riak_pipe_w_xform,
        arg=fun(Input, Partition, FittingDetails) ->
           Results = filter(Input, Where),
           send_results(Results, Partition, FittingDetails)
        end,
        chashfun=follow,
        q_limit=100,
        nval=1}];
where_fitting(_) ->
    [].

select_fitting(<<"*">>) ->
    [];
select_fitting(Select) ->
    [#fitting_spec{name=select,
        module = riak_pipe_w_xform,
        arg=fun(Input, Partition, FittingDetails) ->
           Results = map(Input, Select),
           send_results(Results, Partition, FittingDetails)
        end,
        chashfun=follow,
        q_limit=100,
        nval=1}].

order_by_fittings({Field, Sort}, From) ->
    SortFun = fun({_KeyA, ValueA}, {_KeyB, ValueB}) ->
            FieldA = get_key(Field, ValueA),
            FieldB = get_key(Field, ValueB),
            case Sort of
                asc ->
                    FieldA < FieldB;
                desc ->
                    FieldA > FieldB
            end
    end,
    [#fitting_spec{name={tag, From},
        module = riak_pipe_w_xform,
        arg=fun(Input, Partition, FittingDetails) ->
            send_results([{From, Input}], Partition, FittingDetails)
        end,
        chashfun=follow,
        q_limit=100,
        nval=1},
    #fitting_spec{name=sort_follow,
        module = riak_pipe_w_reduce,
        arg=fun(_Key, [Input|Acc], _P, _FD) ->
            {ok, lists:umerge(SortFun, [Input], Acc)}
        end,
        chashfun=follow,
        q_limit=100,
        nval=1},
    #fitting_spec{name=sort,
        module = riak_pipe_w_reduce,
        arg=fun(_Key, [Input|Acc], _P, _FD) ->
            {ok, lists:umerge(SortFun, Input, Acc)}
        end,
        chashfun=chash:key_of(now()),
        q_limit=100,
        nval=1}];
order_by_fittings(_, _From) ->
    [].

send_results([], _, _) ->
    ok;
send_results([Result|Results], P, FD)  ->
    case riak_pipe_vnode_worker:send_output(Result, P, FD) of
        ok ->
            send_results(Results, P, FD);
        ER ->
            throw(ER)
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

map({Key,Value}, Select) ->
    [{Key, select_keys(Select, Value)}].

filter({_Key,Value}=Input, WhereClause) ->
    case where(WhereClause, Value) of
        false -> [];
        true -> [Input]
    end.

where({'=', Key, Value}, Input) ->
    get_key(Key, Input) == Value;
where({'!=', Key, Value}, Input) ->
    get_key(Key, Input) /= Value;
where({'<', Key, Value}, Input) ->
    get_key(Key, Input) < Value;
where({'>', Key, Value}, Input) ->
    get_key(Key, Input) > Value;
where({'<=', Key, Value}, Input) ->
    get_key(Key, Input) =< Value;
where({'>=', Key, Value}, Input) ->
    get_key(Key, Input) >= Value;
where({'or', Expr1, Expr2}, Input) ->
    where(Expr1, Input) orelse where(Expr2, Input);
where({'and', Expr1, Expr2}, Input) ->
    where(Expr1, Input) andalso where(Expr2, Input).

get_key(Key, {struct, PList}) ->
    case lists:keyfind(Key, 1, PList) of
        false -> null;
        {Key, Value} -> Value
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
