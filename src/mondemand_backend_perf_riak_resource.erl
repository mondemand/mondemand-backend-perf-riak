-module (mondemand_backend_perf_riak_resource).

-export ([ init/1 ]).
-export ([ allowed_methods/2,
           resource_exists/2,
           content_types_provided/2,
           to_json/2,
           to_javascript/2
         ]).

-record (state, {type, data}).

-include_lib ("webmachine/include/webmachine.hrl").

init (_Config) ->
  {ok, #state { }}.

allowed_methods (ReqData, State) ->
  {['GET'], ReqData, State}.

resource_exists (ReqData, State = #state { }) ->
  PathInfo = wrq:path_info (ReqData),
  case PathInfo of
    [ { id, PerfId } ] ->
      {true, ReqData, State#state { type = perf_id, data = PerfId}};
    [ { action, Action } ] ->
      {true, ReqData, State#state { type = admin, data = Action}};
    PI ->
      case wrq:path (ReqData) of
        "/index" ++ _ ->
          {true, ReqData, State#state { type = index,
                                        data = wrq:disp_path (ReqData)}};
        P ->
          io:format ("Unhandled : PathInfo(~p) : Path(~p) ~n",[PI, P]),
          {false, ReqData, State}
      end
  end.

% order matters here, if you put text/javascript first, you will get a 500
% as it will use that if a content type is not specified and it will require
% a jsonp query arg, if we list application/json first then hitting this
% with a browser and without jsonp will work
content_types_provided (ReqData, State) ->
  { [ {"application/json", to_json},
      {"text/javascript", to_javascript}
    ],
    ReqData, State
  }.

to_javascript (ReqData, State) ->
  case get_jsonp (ReqData) of
    undefined ->
      % in order to get javascript you need to specify a callback, otherwise
      % it's considered an error
      { { halt, 500}, ReqData, State };
    _ ->
      to_json (ReqData, State)
  end.

to_json (ReqData,
         State = #state { type = Type, data = Data }) ->

  JSON =
    case Type of
      perf_id -> process_trace (Data, State);
      index -> process_index (Data, State);
      _ -> "{}"
    end,

  ResponseBody =
    case get_jsonp (ReqData) of
      undefined -> JSON;
      Callback -> [ Callback, "(", JSON, ");" ]
    end,

  { ResponseBody, ReqData, State }.

get_jsonp (ReqData) ->
  case wrq:get_qs_value ("jsonp", ReqData) of
    undefined -> wrq:get_qs_value ("callback", ReqData);
    V -> V
  end.

process_trace (Id, _State) ->
  [
    "{\"id\":\"",Id,"\",",
      case mondemand_backend_riak_util:lookup_traces (Id) of
        {ok, R, T} ->
          [ "\"references\":",list_to_json(R),","
            "\"traces\":","[", mondemand_util:join (T, ","), "]"
          ];
        {error, notfound} ->
          [ "\"error\":\"not found\"" ];
        {error, E} -> 
          io_lib:format ("\"error\":\"~p\"",[E])
      end,
    "}"
  ].

process_index ("", _State) ->
  [ "{\"indexes\":",
      list_to_json (mondemand_backend_riak_util:list_indexes()),
    "}"
  ];
process_index (Index, _State) ->
  case mondemand_backend_riak_util:lookup_perf_ids (Index) of
    {ok, P} ->
       [ "{\"index\":\"",Index,"\",",
          "\"traces\":",list_to_json (P),
         "}"
       ];
    {error, notfound} ->
       [ "{\"index\":\"",Index,"\",",
          "\"error\":\"not found\"",
         "}"
       ];
    {error, E} ->
       [ "{\"index\":\"",Index,"\",",
          "\"error\":\"",io_lib:format ("~p",[E]),"\"",
         "}"
       ]
  end.

list_to_json (L) ->
  [ "[",
     mondemand_util:join (lists:map (fun(E) -> ["\"",E,"\""] end, L), ","),
    "]"
  ].
