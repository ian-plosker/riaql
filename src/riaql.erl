-module(riaql).
-export([q/1,q/2,q/3]).

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

map_key_value(RObj, _KD, [{select, Select}, {where, Where}]) ->
    case RObj of
        {error, notfound} ->
            [];
        _ ->
            DJson = mochijson2:decode(riak_object:get_value(RObj)),
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
            end
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
