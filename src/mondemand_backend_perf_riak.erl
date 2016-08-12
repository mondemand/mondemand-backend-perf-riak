-module (mondemand_backend_perf_riak).

-behaviour (supervisor).
-behaviour (mondemand_server_backend).
-behaviour (mondemand_backend_worker).

% Test example
% mondemand-tool -o lwes::127.0.0.1:27572 -X foo:bar -x time1:1464296506692:1464296508643 -c host:`hostname` -c adunit:56
% lwes-journal-emitter -o lwes::127.0.0.1:27572 -n 100 -t ~/all_events.log.20160223100701.1456221961766.1456221977440.gz

%% mondemand_backend_worker callbacks
-export ([ create/1,
           connected/1,
           connect/1,
           send/2,
           destroy/1 ]).

%% mondemand_server_backend callbacks
-export ([ start_link/1,
           process/1,
           required_apps/0,
           type/0
         ]).

%% supervisor callbacks
-export ([ init/1 ]).

-define (POOL, md_perf_riak_pool).
-record (state, { host,
                  port,
                  pid,
                  datetimes = [],
                  context_indices = [],
                  check_interval,
                  check_time
                }).

-define (PERF_ID_BUCKET, {<<"perfid_to_traces">>,<<"perfid_to_traces">>}).

%%====================================================================
%% mondemand_server_backend callbacks
%%====================================================================
start_link (Config) ->
  supervisor:start_link ( { local, ?MODULE }, ?MODULE, [Config]).

process (Event) ->
  mondemand_backend_worker_pool_sup:process (?POOL, Event).

required_apps () ->
  [ protobuffs, riak_pb, riakc ].

type () ->
  supervisor.

%%====================================================================
%% supervisor callbacks
%%====================================================================
init ([Config]) ->
  % default to one process per scheduler
  Number = proplists:get_value (number, Config, erlang:system_info(schedulers)),
  mondemand_server_stats:create_backend (?MODULE, index_adds),
  mondemand_server_stats:create_backend (?MODULE, index_errs),

  { ok,
    {
      {one_for_one, 10, 10},
      [
        { mondemand_backend_perf_riak_indexes,
          { mondemand_backend_perf_riak_indexes, start_link,
            [Config]
          },
          permanent,
          2000,
          worker,
          [ ]
        },
        { mondemand_backend_perf_riak_cleaner,
          { mondemand_backend_perf_riak_cleaner, start_link,
            [ ]
          },
          permanent,
          2000,
          worker,
          [ ]
        },
        { ?POOL,
          { mondemand_backend_worker_pool_sup, start_link,
            [ ?POOL,
              mondemand_backend_worker,
              Number,
              ?MODULE ]
          },
          permanent,
          2000,
          supervisor,
          [ ]
        }
      ]
    }
  }.

%%====================================================================
%% mondemand_backend_worker callbacks
%%====================================================================
create (Config) ->
  Host = proplists:get_value (host, Config, "127.0.0.1"),
  Port = proplists:get_value (port, Config, 8087),
  % should be in millis
  CheckInterval =
    proplists:get_value (index_file_check_interval, Config, 60) * 1000,
  {Indices, Datetimes} = mondemand_backend_perf_riak_indexes:get_indices(),
  {ok, #state { host = Host,
                port = Port,
                datetimes = Datetimes,
                context_indices = Indices,
                check_interval = CheckInterval,
                check_time = mondemand_util:millis_since_epoch() + CheckInterval
              }}.

connected (#state { pid = undefined }) ->
  false;
connected (#state { pid = Pid }) ->
  case mondemand_backend_riak_util:connected (Pid) of
    true -> true;
    {false, Failure} ->
       error_logger:error_msg ("riak not connected ~p",[Failure]),
       false
  end.

connect (State = #state { host = Host, port = Port, pid = undefined }) ->
  Pid = mondemand_backend_riak_util:create_client (Host, Port),
  {ok, State#state { pid = Pid }};
connect (State) ->
  {ok, State}.

send (State = #state { pid = undefined }, _) ->
  {error, State};
send (State = #state { pid = Pid,
                       datetimes = DatetimesIn,
                       context_indices = IndicesIn,
                       check_interval = CheckInterval,
                       check_time = CheckTime
                     },
      Data) ->
  % use the timestamp of the event to see if it might be time to check
  % for new indices
  Timestamp = mondemand_event:receipt_time (Data),

  % if we have new indices set them in the state
  {Indices, DateTimes, NewState} =
    case CheckTime >= Timestamp of
      true ->
        error_logger:info_msg("Refetch indices",[]),
        {NewIndices, NewDatetimes} =
          mondemand_backend_perf_riak_indexes:get_indices(),
        {NewIndices, NewDatetimes,
         State#state { datetimes = NewDatetimes,
                       context_indices = NewIndices,
                       check_time = Timestamp + CheckInterval }};
      false ->
        {IndicesIn, DatetimesIn, State}
    end,

  % convert messages to json
  PerfMsg = mondemand_event:msg (Data),
  Id = mondemand_perfmsg:id(PerfMsg),
  Context = mondemand_perfmsg:context (PerfMsg),
  JSON =
    list_to_binary(
      lwes_mochijson2:encode (
           {
            [
             {sender_ip, lwes_util:ip2bin(mondemand_event:sender_ip (Data))},
             {id, Id},
             {context, {Context}},
             {caller_label, mondemand_perfmsg:caller_label (PerfMsg)},
             {timings,
              [ {[ { label, mondemand_perfmsg:timing_label (L)},
                   { start_time, mondemand_perfmsg:timing_start_time (L)},
                   { end_time, mondemand_perfmsg:timing_end_time (L)}
                 ]} || L <- mondemand_perfmsg:timings (PerfMsg)
              ]
             }
            ]
           }
      )
    ),

  DateTimeList = calculate_datetimes (Timestamp, DateTimes),
  IndexesToAdd = calculate_indexes (Context, DateTimeList, Indices),
  {Added, Errors, BinIndexesToAdd} = process_indexes (Pid, IndexesToAdd, Id),
  mondemand_server_stats:increment_backend(?MODULE, index_adds, Added),
  mondemand_server_stats:increment_backend(?MODULE, index_errs, Errors),

  case
    mondemand_backend_riak_util:add_to_traces (Pid, Id, BinIndexesToAdd, JSON)
  of
    ok ->
      {ok, NewState};
    {error, UE} ->
      error_logger:error_msg ("Update failure ~p",[UE]),
      { error, NewState }
  end.

destroy (#state { pid = undefined }) ->
  ok;
destroy (#state { pid = Pid }) ->
  mondemand_backend_riak_util:destroy_client (Pid),
  ok.

calculate_datetimes (Timestamp, DateTimes) ->
  ErlangNow = mondemand_util:millis_epoch_to_erlang_now (Timestamp),
  {{Year, Month, Day}, {Hour, Minute, _}} =
    mondemand_util:now_to_mdyhms (ErlangNow),
  lists:map (fun (minute) ->
                   { minute,
                     list_to_binary(
                       io_lib:fwrite ("~4..0B-~2..0B-~2..0BT~2..0B:~2..0B",
                                      [Year, Month, Day, Hour, Minute]))
                   };
                 (hour) ->
                   { hour,
                     list_to_binary(
                       io_lib:fwrite ("~4..0B-~2..0B-~2..0BT~2..0B",
                                      [Year, Month, Day, Hour]))
                   };
                 (day) ->
                   { day,
                     list_to_binary(
                       io_lib:fwrite ("~4..0B-~2..0B-~2..0B",
                                      [Year, Month, Day]))
                   }
             end,
             DateTimes).

calculate_indexes (Context, DateTimeList, Indexes) ->
  % Indexes is of the form
  % [ {index, IndexList, Lifetime} ]
  % so we map, over those with a fold to keep track of errors
  lists:map (
    fun ({index, I, _}) ->
      lists:mapfoldl (fun ({datetime,T}, A) ->
                            case lists:keyfind (T, 1, DateTimeList) of
                              false -> {datetime, [ missing_datetime | A]};
                              {_,DateTime} -> {DateTime, A}
                            end;
                          ({K,'*'},A) ->
                            case lists:keyfind (K, 1, Context) of
                              false ->  {K, [ missing_key | A ]};
                              {_, V} -> {V, A}
                            end;
                          ({K,V},A) ->
                            case lists:keyfind (K, 1, Context) of
                              false ->  {K, [ missing_key | A ]};
                              {_, V} -> {V, A};
                              {K, _} -> {K, [ wrong_value | A ]}
                            end
                      end,
                      [],
                      I)
    end,
    Indexes).

process_indexes (Pid, ToAdd, PerfId) ->
  lists:foldl(
    fun ({IndexKeys,[]},{G,B,A}) ->
          Index = list_to_binary(mondemand_server_util:join (IndexKeys,"/")),
          case mondemand_backend_riak_util:add_to_index (Pid, Index, PerfId)
          of
            ok -> { G + 1, B, [Index | A]};
            error -> { G, B + 1, A }
          end;
        (_,{G,B,A}) ->
          { G, B + 1,A}
    end,
    {0,0,[]},
    ToAdd).
