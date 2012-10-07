-module(riaql).
-export([q/1,q/2,q/3]).

q(<<_/bitstring>>=Query) ->
    parse(Query);
q(From) ->
    q(From, <<"*">>).
q(From, Select) ->
    q(From, Select, none).
q(From, Select, Where) ->
    {ok, Client} = riak:local_client(),
    case Client:mapred(
                       From,
                       [{
                         map,
                         {qfun, fun map_key_value/3},
                         [{select, Select}, {where, Where}],
                         true
                        }],
                        60000) of
        {ok, Result} -> Result;
        {error, Reason} -> throw(Reason)
    end.

parse(Query) ->
    Tokens = lists:filter(
                          fun(Val) -> Val =/= <<>> end,
                          binary:split(Query, <<" ">>, [global, trim])
                         ),
    case parse_tokens(Tokens) of
        [Select, From, Where] ->
            q(From, Select, Where);
        [Select, From] ->
            q(From, Select);
        _ ->
            throw(bad_pattern)
    end.
parse_tokens([<<"SELECT">>|Rest]) ->
    {SelectTokens, Tail} = lists:splitwith(fun(Val) -> Val =/= <<"FROM">> end, Rest),
    Select = parse_select(SelectTokens, []),
    [Select|parse_tokens(Tail)];
parse_tokens([<<"FROM">>|Rest]) ->
    {FromTokens, Tail} = lists:splitwith(fun(Val) -> Val =/= <<"WHERE">> end, Rest),
    From = parse_from(FromTokens, []),
    [From|parse_tokens(Tail)];
parse_tokens([<<"WHERE">>|Rest]) ->
    [parse_where(Rest, [])];
parse_tokens(_) ->
    [].

parse_select([<<"*">>], _) ->
    <<"*">>;
parse_select([], Select) ->
    lists:reverse(Select);
parse_select(Tokens=[_|_], Select) ->
    {Head, Tail} = lists:splitwith(fun(Val) -> Val =/= <<",">> end, Tokens),
    parse_select(Tail, [case Head of
        [Key] ->
            {Key, Key};
        [MapKey, <<"=">>, Key] ->
            {MapKey, Key};
        _ ->
            throw(bad_pattern)
    end|Select]).

parse_from([Bucket], []) ->
    Bucket;
parse_from(_, _) ->
    throw(bad_pattern).

parse_where([], Where) ->
    Where;
parse_where(Tokens=[_|_], Where) ->
    {Head, Tail} = lists:splitwith(fun(Val) -> Val =/= <<",">> end, Tokens),
    parse_select(Tail, [case Head of
        [Key, <<"=">>, Val1] ->
            {Key, fun(Val2) -> Val1 =:= Val2 end};
        _ ->
            throw(bad_pattern)
    end|Where]).

map_key_value({error, notfound}, _, _) ->
    [];
map_key_value(RObj, _KD, [{select, Select}, {where, Where}]) ->
    case (catch mochijson2:decode(riak_object:get_value(RObj))) of
        {'EXIT', _} ->
            [];
        DJson={struct, _} ->
            case Where of
                [{_,_}|_] ->
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
