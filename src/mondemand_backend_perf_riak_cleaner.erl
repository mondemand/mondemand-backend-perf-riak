-module(mondemand_backend_perf_riak_cleaner).

-behaviour(gen_server).

%% API
-export([start_link/0,
         delete_trace/1]).

%% gen_server callbacks

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record (state, {}).

%%====================================================================
%% API functions
%%====================================================================
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

delete_trace (Id) ->
  gen_server:cast (?MODULE, {delete_trace, Id}).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init(_Args) ->
  {ok, #state{}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast({delete_trace, Id}, State) ->
  mondemand_backend_riak_util:delete_trace (Id),
  {noreply, State};
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
