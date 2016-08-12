-module(mondemand_backend_perf_riak_indexes).

-behaviour(gen_server).

%% API
-export([start_link/1,
         load_file/1,
         get_indices/0,
         reload/0]).

%% gen_server callbacks

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-define (DEFAULT_INDICES, {[{index,[{datetime,minute}],60}],[minute]}).
-record (state, { file,
                  interval,
                  delay,
                  timer,
                  last_scan,
                  indices
                }).

%%====================================================================
%% API functions
%%====================================================================
start_link(Config) ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [Config], []).

load_file (File) ->
  gen_server:cast (?MODULE, {load, File}).

get_indices() ->
  gen_server:call (?MODULE, {get}).

reload() ->
  gen_server:cast (?MODULE, {load}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Config]) ->
  IndexFile =
    proplists:get_value (index_file, Config, undefined),
  IndexFileCheckInterval =
    proplists:get_value (index_file_check_interval, Config, 60),
  % interval time is in seconds, but gen_server timeouts are millis
  Delay = IndexFileCheckInterval * 1000,
  {ok, TRef} = timer:apply_interval(Delay, ?MODULE, reload, []),
  State = #state {
            file = IndexFile,
            interval = IndexFileCheckInterval,
            delay = Delay,
            timer = TRef,
            last_scan = [],
            indices = ?DEFAULT_INDICES
          },
  {ok, load (State)}.

handle_call({get}, _From,
            State = #state { indices = Indices }) ->
  {reply, Indices, State};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({load, File}, State) ->
  % allow for changing of the file
  {noreply, load(State#state { file = File})};
handle_cast({load}, State) ->
  % check and possibly load
  {noreply, load(State)};
handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%------------------------------------------------------------------
%% internal functions
%%------------------------------------------------------------------
load (State = #state { file = IndexFile, last_scan = LastScan}) ->
  NewScan = lwes_file_watcher:scan ([IndexFile]),
  case lwes_file_watcher:changes (LastScan, NewScan) of
    [] ->
      % no changes
      error_logger:info_msg ("No changes in ~p",[IndexFile]),
      State#state { last_scan = NewScan };
    _ ->
      case read_index_file (IndexFile) of
        error ->
          State#state { last_scan = NewScan };
        NewIndices ->
          error_logger:info_msg ("Found changes in ~p",[IndexFile]),
          State#state { last_scan = NewScan, indices = NewIndices }
      end
  end.

read_index_file (File) ->
  case file:consult (File) of
    {error, enoent} ->
      error_logger:error_msg ("Failed to find index file ~p",[File]),
      error;
    {error, O} ->
      error_logger:error_msg ("Found errors in ~p, not loading : ~p",[File, O]),
      error;
    {ok, [Indices]} ->
      case validate_indices(Indices) of
        false ->
          error_logger:error_msg ("Failed to validate indices from ~p",[File]),
          error;
        true ->
          normalize_indices (Indices)
      end
  end.

validate_indices (L) when is_list (L) ->
  lists:all(fun validate_index/1, L).

% an index is valid if it contains all valid entries and only a one or zero
% datetime entries
validate_index ({index, I, N}) when is_list(I), is_integer(N) ->
  lists:all(fun validate_entry/1,I) andalso length(datetime(I)) =< 1;
validate_index (_) -> false.

validate_entry ({datetime, minute}) -> true;
validate_entry ({datetime, hour}) -> true;
validate_entry ({datetime, day}) -> true;
validate_entry ({datetime, _}) -> false;
validate_entry ({_, '*'}) -> true;
validate_entry ({_, _}) -> true;
validate_entry (_) -> false.

datetime (L) ->
  lists:foldl(fun({datetime,V},A) -> [V | A];
                 (_,A) -> A
              end, [], L).

normalize_indices (L) ->
  {NewIndices, LoLDatetimes} =
    lists:mapfoldl (fun ({index, I, N}, ListOfDateTimeOptions) ->
                      {{index, lists:map(fun normalize_entry/1, I), N},
                       [ datetime(I) | ListOfDateTimeOptions ]}
                    end,
                    [],
                    L),
  {NewIndices,lists:usort(lists:flatten (LoLDatetimes))}.

normalize_entry (E = {datetime, _}) -> E;
normalize_entry ({K,'*'}) -> { mondemand_util:binaryify(K), '*' };
normalize_entry ({K,V}) ->
  { mondemand_util:binaryify(K), mondemand_util:binaryify(V) }.
