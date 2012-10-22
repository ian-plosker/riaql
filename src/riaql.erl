-module(riaql).
-export([q/1]).

q(<<_/bitstring>>=Query) ->
    q(riaql_parser:parse(Query));
q(Query) when is_list(Query) ->
    riaql_pipe:process(Query).
