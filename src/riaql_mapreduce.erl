-module(riaql_mapreduce).
-export([mapred/3]).

-record(fitting,
        {
          pid :: pid(),
          ref :: reference(),
          chashfun :: riak_pipe_vnode:chashfun(),
          nval :: riak_pipe_vnode:nval()
        }).

-record(pipe,
        {
          builder :: pid(),
          fittings :: [{Name::term(), #fitting{}}],
          sink :: #fitting{}
        }).

-record(pipe_result,
        {
          ref,
          from,
          result
        }).

-record(pipe_eoi,
        {
          ref
        }).

-record(pipe_log,
        {
          ref,
          from,
          msg
        }).

mapred(Inputs, Query, Timeout) ->
    case riak_kv_mrc_pipe:mapred_stream(Query) of
        {{ok, Pipe}, NumKeeps} ->
            PipeRef = (Pipe#pipe.sink)#fitting.ref,
            Tref = erlang:send_after(Timeout, self(), {pipe_timeout, PipeRef}),
            {InputSender, SenderMonitor} =
                riak_kv_mrc_pipe:send_inputs_async(Pipe, Inputs),
                pipe_mapred_nonchunked(Pipe, NumKeeps,
                                       {InputSender, SenderMonitor}, {Tref, PipeRef});
        _ -> throw(fail)
    end.

pipe_mapred_nonchunked(Pipe, NumKeeps, Sender, PipeTref) ->
    case pipe_collect_outputs(Pipe, NumKeeps, Sender) of
        {ok, Results} ->
            JSONResults =
                case NumKeeps < 2 of
                    true ->
                        [riak_kv_mapred_json:jsonify_not_found(R)
                         || R <- Results];
                    false ->
                        [[riak_kv_mapred_json:jsonify_not_found(PR)
                          || PR <- PhaseResults]
                         || PhaseResults <- Results]
                end,
            JSONResults1 = riak_kv_mapred_json:jsonify_bkeys(JSONResults, true),
            cleanup_timer(PipeTref),
            {ok, JSONResults1};
        {error, Error} ->
            cleanup_timer(PipeTref),
            riak_pipe:destroy(Pipe),
            throw({error, Error})
    end.

pipe_collect_outputs(Pipe, NumKeeps, Sender) ->
    Ref = (Pipe#pipe.sink)#fitting.ref,
    case pipe_collect_outputs1(Ref, Sender, []) of
        {ok, Outputs} ->
            {ok, riak_kv_mrc_pipe:group_outputs(Outputs, NumKeeps)};
        Error ->
            Error
    end.

pipe_collect_outputs1(Ref, Sender, Acc) ->
    case pipe_receive_output(Ref, Sender) of
        {ok, Output} -> pipe_collect_outputs1(Ref, Sender, [Output|Acc]);
        eoi          -> {ok, lists:reverse(Acc)};
        Error        -> Error
    end.

pipe_receive_output(Ref, {SenderPid, SenderRef}) ->
    receive
        #pipe_eoi{ref=Ref} ->
            eoi;
        #pipe_result{ref=Ref, from=From, result=Result} ->
            {ok, {From, Result}};
        #pipe_log{ref=Ref, from=From, msg=Msg} ->
            case Msg of
                {trace, [error], {error, Info}} ->
                    {error, riak_kv_mapred_json:jsonify_pipe_error(
                              From, Info)};
                _ ->
                    %% not a log message we're interested in
                    pipe_receive_output(Ref, {SenderPid, SenderRef})
            end;
        {'DOWN', SenderRef, process, SenderPid, Reason} ->
            if Reason == normal ->
                    %% just done sending inputs, nothing to worry about
                    pipe_receive_output(Ref, {SenderPid, SenderRef});
               true ->
                    {error, {sender_error, Reason}}
            end;
        {pipe_timeout, Ref} ->
            {error, timeout}
    end.

cleanup_timer({Tref, PipeRef}) ->
    case erlang:cancel_timer(Tref) of
        false ->
            receive
                {pipe_timeout, PipeRef} ->
                    ok
            after 0 ->
                    ok
            end;
        _ ->
            ok
    end.
