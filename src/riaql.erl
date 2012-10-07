-module(riaql).
-export([q/1,q/2]).
-export([keys/1]).

keys(BKeys=[{_Bucket,_Key}|_Rest]) ->
    BKeys.

q(From) ->
    q(From, <<"*">>).
q(From, Map={map, _, _, _}) ->
    {ok, Client} = riak:local_client(),
    case Client:mapred(
            From,
            [
                Map%,
                %{reduce, {qfun, Where}, none, true}
            ],
            60000) of
        {ok, Result} -> Result;
        {error, Reason} -> throw(Reason)
    end;
q(From, Select) ->
    q(From, {map, {qfun, fun map_key_value/3}, Select, true}).

map_key_value(RObj, _KD, Select) ->
    case RObj of
        {error, notfound} ->
            [];
        _ ->
            [{riak_object:key(RObj), select_keys(Select, riak_object:get_value(RObj))}]
    end.

select_keys(<<"*">>, BJson) ->
    mochijson2:decode(BJson);
select_keys(Keys=[{_,_}|_Rest], BJson) ->
    {struct, PList} = mochijson2:decode(BJson),
    {struct, lists:foldl(fun({Key, Val}, Acc) ->
        case lists:keyfind(Key, 2, Keys) of
            false -> Acc;
            {NewKey, Key} -> [{NewKey, Val}|Acc]
        end
    end, [], PList)}.

