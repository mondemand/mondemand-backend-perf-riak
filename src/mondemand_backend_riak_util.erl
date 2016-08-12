-module (mondemand_backend_riak_util).

-export ([ create_client/0,
           create_client/2,
           destroy_client/1,
           connected/1,
           add_to_traces/4,
           add_to_index/3,
           lookup_traces/1,
           lookup_perf_ids/1,
           count_traces/0,
           clear_traces/0,
           delete_trace/1,
           delete_trace/2,
           count_indexes/0,
           clear_indexes/0,
           delete_index/1,
           list_indexes/0,
           clear_all/0
         ]).

-define (PERF_ID_BUCKET, {<<"perfid_to_traces">>,<<"perfid_to_traces">>}).
-define (INDEXES_BUCKET, {<<"indexes">>, <<"indexes">>}).

create_client () ->
  Config = mondemand_server_config:backend_config (mondemand_backend_perf_riak),
  Host = proplists:get_value (host, Config, "127.0.0.1"),
  Port = proplists:get_value (port, Config, 8087),
  create_client (Host, Port).

create_client (Host, Port) ->
  {ok, Pid} = riakc_pb_socket:start (Host, Port),
  Pid.

destroy_client (Pid) ->
  riakc_pb_socket:stop (Pid).

connected (Pid) ->
  riakc_pb_socket:is_connected (Pid).

delete_index_from_trace (Pid, PerfId, Index) ->
  riakc_pb_socket:modify_type (Pid,
                               fun (OldMap) ->
                                 riakc_map:update (
                                   {<<"references">>,set},
                                   fun(OldSet) ->
                                     riakc_set:del_element (Index,OldSet)
                                   end,
                                   OldMap)
                               end,
                               ?PERF_ID_BUCKET,
                               PerfId,
                               [return_body]).

add_to_traces (Pid, PerfId, IndexesToAdd, PerfTrace) ->
  riakc_pb_socket:modify_type (Pid,
                               fun (OldMap) ->
                                 MapWithRefCount =
                                   riakc_map:update (
                                     {<<"references">>,set},
                                     fun(S) ->
                                       lists:foldl(
                                         fun(I,A) ->
                                           riakc_set:add_element(I, A)
                                         end,
                                         S,
                                         IndexesToAdd
                                       )
                                     end,
                                     OldMap),
                                 MapWithTraceAdded =
                                   riakc_map:update (
                                     {<<"traces">>,set},
                                     fun (S) ->
                                       riakc_set:add_element (PerfTrace, S)
                                     end,
                                     MapWithRefCount),
                                 MapWithTraceAdded
                               end,
                               ?PERF_ID_BUCKET,
                               PerfId,
                               [create]).

add_to_index (Pid, Index, PerfId) ->
  case riakc_pb_socket:modify_type (Pid,
                                    fun (OldSet) ->
                                      riakc_set:add_element (PerfId, OldSet)
                                    end,
                                    ?INDEXES_BUCKET,
                                    Index,
                                    [create])
  of
    ok -> ok;
    {error, E} ->
      error_logger:error_msg ("Unable to add ~p to index ~p : ~p",
                              [PerfId, Index, E]),
      error
  end.

count_traces () ->
  keysfold (?PERF_ID_BUCKET,
            fun (_, _, Accum) -> Accum + 1 end,
            0).

clear_traces () ->
  keysfold (?PERF_ID_BUCKET,
            fun (Pid, Key, Accum) ->
              delete_trace (Pid, Key),
              Accum + 1
            end,
            0).

delete_trace (Key) ->
  Pid = create_client (),
  Result = delete_trace (Pid, Key),
  destroy_client (Pid),
  Result.

delete_trace (Pid, Key) when is_list (Key) ->
  delete_trace (Pid, list_to_binary (Key));
delete_trace (Pid, Key) ->
  riakc_pb_socket:delete(Pid, ?PERF_ID_BUCKET, Key).

count_indexes () ->
  keysfold (?INDEXES_BUCKET,
            fun (_, _, Accum) -> Accum + 1 end,
            0).

clear_indexes () ->
  keysfold (?INDEXES_BUCKET,
            fun (Pid, Key, Accum) ->
              riakc_pb_socket:delete(Pid, ?INDEXES_BUCKET, Key),
              Accum + 1
            end,
            0).

delete_index (Index) when is_list (Index) ->
  delete_index (list_to_binary (Index));
delete_index (Index) ->
  Pid = create_client (),
  Result =
    case lookup_perf_ids (Pid, Index) of
      {ok, PerfIds} ->
        [ case delete_index_from_trace (Pid, PerfId, Index) of
            {ok, V} ->
              case get_references (V) =:= [] of
                true ->
                  mondemand_backend_perf_riak_cleaner:delete_trace (PerfId);
                false ->
                  ok
              end;
            E ->
              error_logger:error_msg ("delete_index_from_trace (~p,~p) : failed with ~p",[PerfId,Index,E])
          end
          || PerfId
          <- PerfIds
        ],
        riakc_pb_socket:delete(Pid, ?INDEXES_BUCKET, Index);
      {error, notfound} ->
        ok;
      E ->
        error_logger:error_msg ("failed to find perf_ids for index ~p",[Index]),
        E
    end,
  destroy_client (Pid),
  Result.

list_indexes () ->
  keysfold (?INDEXES_BUCKET,
            fun (_, Key, A) ->
              [Key | A]
            end,
            []).

clear_all () ->
  clear_indexes() + clear_traces().

get_references (Map) -> riakc_map:fetch({<<"references">>,set},Map).
get_traces (Map) -> riakc_map:fetch({<<"traces">>,set},Map).

lookup_traces (PerfId) when is_list (PerfId) ->
  lookup_traces (list_to_binary (PerfId));
lookup_traces (PerfId) when is_binary (PerfId) ->
  Pid = create_client (),
  Result =
    case riakc_pb_socket:fetch_type (Pid, ?PERF_ID_BUCKET, PerfId) of
      {ok, M} -> {ok, get_references (M), get_traces (M) };
      {error, {notfound, map}} -> {error, notfound};
      {error, E} -> {error, E}
    end,
  destroy_client (Pid),
  Result.

lookup_perf_ids (Index) when is_list (Index) ->
  lookup_perf_ids (list_to_binary (Index));
lookup_perf_ids (Index) when is_binary (Index) ->
  Pid = create_client (),
  Result = lookup_perf_ids (Pid, Index),
  destroy_client (Pid),
  Result.

lookup_perf_ids (Pid, Index) ->
  case riakc_pb_socket:fetch_type (Pid, ?INDEXES_BUCKET, Index) of
  {ok, S} -> {ok, riakc_set:value (S)};
    {error, {notfound, set}} -> {error, notfound};
    {error, E} -> {error, E}
  end.

keysfold (Bucket, Fun, Accum0) ->
  Pid = create_client (),
  Result =
    case riakc_pb_socket:stream_list_keys (Pid, Bucket) of
      {ok, ReqId} ->
        wait_and_fold (Pid, ReqId, Fun, Accum0);
      Error ->
        Error
    end,
  destroy_client (Pid),
  Result.

wait_and_fold (Pid, ReqId, Fun, Accum) ->
  receive
    {ReqId, done} -> Accum;
    {ReqId, {error, Reason}} -> {error, Reason, Accum};
    {ReqId, {keys, Keys}} ->
      NewAccum = lists:foldl (fun (K, A) ->
                                Fun (Pid, K, A)
                              end, Accum, Keys),
      wait_and_fold (Pid, ReqId, Fun, NewAccum)
  end.
