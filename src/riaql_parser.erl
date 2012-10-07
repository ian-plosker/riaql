-module(riaql_parser).
-export([parse/1]).

parse(Query) ->
    Tokens = lists:filter(
                          fun(Val) -> Val =/= <<>> end,
                          binary:split(Query, <<" ">>, [global, trim])
                         ),
    case parse_tokens(Tokens) of
        [Select, From, Where] ->
            {From, Select, Where};
        [Select, From] ->
            {From, Select};
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
