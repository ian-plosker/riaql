-module(riaql).
-export([q/1,q/2,q/3]).
-export([bucket/1,index/3,index/4]).

q(<<_/bitstring>>=Query) ->
    case riaql_parser:parse(Query) of
        {From, Select} ->
            q(From, Select);
        {From, Select, Where} ->
            q(From, Select, Where)
    end;
q(From) ->
    q(From, <<"*">>).
q(From, Select) ->
    q(From, Select, none).
q(From, Select, Where) ->
    riaql_pipe:process(From, Select, Where).

bucket(Bucket) ->
    Bucket.

index(Bucket, Index, Key) ->
    {index, Bucket, Index, Key}.
index(Bucket, Index, StartKey, EndKey) ->
    {index, Bucket, Index, StartKey, EndKey}.
