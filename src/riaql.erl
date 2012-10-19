-module(riaql).
-export([q/1]).
-export([bucket/1,index/3,index/4]).

q(<<_/bitstring>>=Query) ->
    q(riaql_parser:parse(Query));
q(Query) when is_list(Query) ->
    riaql_pipe:process(Query).

bucket(Bucket) ->
    Bucket.

index(Bucket, Index, Key) ->
    {index, Bucket, Index, Key}.
index(Bucket, Index, StartKey, EndKey) ->
    {index, Bucket, Index, StartKey, EndKey}.
