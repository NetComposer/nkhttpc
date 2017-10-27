%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Single connection server
-module(nkhttpc_single).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([start_link/2, start/2, stop/1, req/6, get_all/0]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, 
         handle_info/2]).
-export_type([conn/0, conn_opts/0]).

-include("nkhttpc.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").


%% ===================================================================
%% Types
%% ===================================================================

-type id() :: term().
-type method() :: atom() | binary().
-type path() :: binary().
-type header() :: {binary(), binary()}.
-type body() :: iolist().
-type status() :: 100..599.

-type conn() :: {tcp|tls, inet:ip_address(), inet:port_number(), conn_opts()}.

-type conn_opts() ::
    #{  
        host => binary(),
        path => binary(),                       % Base path
        headers => [{binary(), binary()}],
        auth => {basic, binary(), binary()},
        idle_timeout => integer(),
        debug => boolean(),
        packet_debug => boolean(),
        refresh_fun => fun((Pid::pid()) -> ok),
        refresh_request => {method(), path(), [header()], body}
    } |
    nkpacket:tls_types().


-define(IDLE_TIMEOUT, 200000).

-define(DEBUG(Txt, Args, State),
    case State#state.debug of
        true -> ?LLOG(debug, Txt, Args, State);
        false -> ok
    end).

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkHTTPc ~s (~s ~p) "++Txt,
               [State#state.id, State#state.host, self()|Args])).


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Starts a http connection
-spec start_link(id(), conn()|[conn()]) ->
    {ok, pid()} | {error, term()}.

start_link(Id, Conns) when is_list(Conns) ->
    gen_server:start_link(?MODULE, [Id, Conns], []);

start_link(Id, Conn) ->
    start_link(Id, [Conn]).


%% @doc Starts a http connection
-spec start([conn()], conn_opts()) ->
    {ok, pid()} | {error, term()}.

start(Id, Conns) when is_list(Conns) ->
    gen_server:start(?MODULE, [Id, Conns], []);

start(Id, Conn) ->
    start(Id, [Conn]).


%% @doc Stops a connection
-spec stop(pid()) ->
    ok.

stop(Pid) ->
    gen_server:cast(Pid, stop).


%% @doc Sends a message
%% Every connection has a timeout, so it will never block
-spec req(pid(), method(), path(), [header()], body(), integer()) ->
    {ok, status(), [header()], binary(), integer()} | {error, term()}.

req(Pid, Method, Path, Hds, Body, Timeout) ->
    Method2 = nklib_util:to_upper(Method),
    Path2 = nklib_util:to_binary(Path),
    Start = nklib_util:m_timestamp(),
    case nklib_util:call(Pid, {req, Method2, Path2, Hds, Body}, Timeout) of
        {error, {exit, {{timeout, _}, _}}} ->
            {error, call_timeout};
        {ok, Status, RepHds, RepBody} ->
            Time = nklib_util:m_timestamp() - Start,
            {ok, Status, RepHds, RepBody, Time};
        {error, Error} ->
            {error, Error}
    end.


get_all() ->
    nklib_proc:values(?MODULE).



%% ===================================================================
%% gen_server
%% ===================================================================


-record(req, {
    ref :: reference(),
    from :: term(),
    status = 0 :: integer(),
    hds = [] :: [{binary(), binary()}],
    chunks = [] :: [term()]
}).

-record(state, {
    id :: term(),
    conns :: list(),
    conn_opts :: map(),
    host :: binary(),
    path :: binary(),
    hds :: [{binary(), binary()}],
    conn_text = <<>> :: binary(),
    conn_pid :: pid(),
    reqs = [] ::[#req{}],
    refresh_interval :: integer(),
    refresh_request :: {method(), path(), [header()], body()},
    debug :: boolean()
}).


%% @private 
-spec init(term()) ->
    {ok, #state{}} | {stop, term()}.

init([Id, Conns]) when is_list(Conns) ->
    nklib_proc:put(?MODULE, Id),
    State = #state{id=Id, conns=Conns},
    case connect(State) of
        {ok, #state{conn_text=Text}=State2} ->
            ?LLOG(info, "new connection (~s)", [Text], State2),
            self() ! refresh,
            {ok, State2};
        {error, Error} ->
            {stop, Error}
    end.


%% @private
-spec handle_call(term(), {pid(), reference()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} | {stop, term(), term(), #state{}}.

handle_call({req, Method, Path, Hds, Body}, From, #state{reqs=Reqs}=State) ->
    #state{hds=BaseHds, path=BasePath, conn_pid=ConnPid} = State,
    Ref = make_ref(),
    Path2 = case BasePath of
        <<>> -> Path;
        _ -> <<BasePath/binary, Path/binary>>
    end,
    Msg = {http, Ref, Method, Path2, BaseHds++Hds, Body},
    ?DEBUG("send ~p", [Msg], State),
    case nkpacket:send(ConnPid, Msg) of
        {ok, _} ->
            Req = #req{
                ref = Ref,
                from = From
            },
            {noreply, State#state{reqs=Reqs++[Req]}};
        {error, Error} ->
            {error, Error, State}
    end;

handle_call(get_state, _From, State) ->
    {reply, State, State};

handle_call(Msg, _From, State) -> 
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}} | {stop, normal, #state{}}.

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(Msg, State) -> 
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info({nkhttpc, Ref, {head, Status, Hds}}, #state{reqs=Reqs}=State) ->
    ?DEBUG("head: ~p, ~p", [Status, Hds], State),
    [#req{ref=Ref}=Req|Rest] = Reqs,
    Req2 = Req#req{status=Status, hds=Hds},
    {noreply, State#state{reqs=[Req2|Rest]}};

handle_info({nkhttpc, Ref, {chunk, Data}}, #state{reqs=Reqs}=State) ->
    % ?DEBUG("chunk: ~p", [Data]),
    [#req{ref=Ref, chunks=Chunks}=Req|Rest] = Reqs,
    Req2 = Req#req{chunks=[Data|Chunks]},
    {noreply, State#state{reqs=[Req2|Rest]}};

handle_info({nkhttpc, Ref, {body, Body}}, #state{reqs=Reqs}=State) ->
    ?DEBUG("body: ~p", [Body], State),
    [#req{ref=Ref, status=Status, hds=Hds, from=From, chunks=Chunks}|Rest]=Reqs,
    Body2 = case Chunks of
        [] ->
            Body;
        _ when Body == <<>> ->
            list_to_binary(lists:reverse(Chunks));
        _ ->
            <<"invalid_chunked">>
    end,
    gen_server:reply(From, {ok, Status, Hds, Body2}),
    {noreply, State#state{reqs=Rest}};

handle_info(refresh, #state{refresh_interval=0}=State) ->
    {noreply, State};

handle_info(refresh, #state{refresh_interval=Interval}=State) ->
    #state{refresh_request={Method, Path, Hds, Body}} = State,
    Self = self(),
    spawn_link(
        fun() ->
            req(Self, Method, Path, Hds, Body, 1000*Interval),
            erlang:send_after(1000*Interval, Self, refresh)
        end),
    {noreply, State};

handle_info({'DOWN', _MRef, process, Pid, Reason}, #state{conn_pid=Pid}=State) ->
    lists:foreach(
        fun(#req{from=From}) -> gen_server:reply(From, {error, process_down}) end,
        State#state.reqs),
    case connect(State#state{reqs=[]}) of
        {ok, #state{conn_text=Text}=State2} ->
            ?DEBUG("reconnected to ~s", [Text], State),
            {noreply, State2};
        {error, _} ->
            case Reason of
                normal ->
                    {stop, normal, State};
                _ ->
                    {stop, {connection_down, Reason}, State}
            end
    end;

handle_info(Info, State) ->
    lager:warning("Module ~p received unexpected info: ~p", [?MODULE, Info]),
    {noreply, State}.


%% @private
-spec code_change(term(), #state{}, term()) ->
    {ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
-spec terminate(term(), #state{}) ->
    ok.

terminate(_Reason, State) ->
    ?DEBUG("terminate (~p)", [self()], State),
    ok.



%% ===================================================================
%% Private
%% ===================================================================

%% @doc
connect(#state{conns=Conns}=State) ->
    connect(nklib_util:randomize(Conns), State).


%% @private
connect([], _State) ->
    {error, no_connections};

connect([{Proto, Ip, Port, Opts}|Rest], #state{id=Id}=State) ->
    TLSKeys = nkpacket_util:tls_keys(),
    TLSOpts = maps:with(TLSKeys, Opts),
    ConnOpts = TLSOpts#{
        id => Id,
        class => {nkhttpc, self()},
        monitor => self(),
        user_state => {notify, self()},
        idle_timeout => maps:get(idle_timeout, Opts, ?IDLE_TIMEOUT),
        debug => maps:get(packet_debug, Opts, false)
    },
    State2 = State#state{
        conn_opts = ConnOpts,
        host = maps:get(host, Opts, <<>>),
        path = nklib_parse:path(maps:get(path, Opts, <<>>)),
        hds = get_headers(Opts),
        debug = maps:get(debug, Opts, false),
        refresh_interval = maps:get(refresh_interval, Opts, 0),
        refresh_request = maps:get(refresh_request, Opts, undefined)
    },
    Conn = #nkconn{protocol=nkhttpc_protocol, transp=Proto, ip=Ip, port=Port, opts=ConnOpts},
    case nkpacket:connect(Conn, ConnOpts) of
        {ok, Pid} ->
            monitor(process, Pid),
            State3 = add_host(Ip, Port, State2),
            State4 = State3#state{
                conn_pid = Pid,
                conn_text = nkpacket_util:get_uri(http, Proto, Ip, Port)
            },
            {ok, State4};
        {error, Error} when Rest==[] ->
            {error, Error};
        {error, _Error} ->
            connect(Rest, State)
    end.


%% @private
get_headers(Opts) ->
    Hds1 = maps:get(headers, Opts, []),
    Hds2 = [{<<"Connection">>, <<"keep-alive">>}|Hds1],
    case Opts of
           #{auth:={basic, User, Pass}} ->
               Auth = base64:encode(list_to_binary([User, ":", Pass])),
               [{<<"Authorization">>, <<"Basic ", Auth/binary>>} | Hds2];
           _ ->
               Hds2
    end.


%% @private
add_host(Ip, Port, #state{hds=Hds, host=Host}=State) ->
    Name = case Host of
               <<>> -> Ip;
               _ -> Host
           end,
    HostHeader = list_to_binary([
        nklib_util:to_host(Name),
        <<":">>,
        nklib_util:to_binary(Port)
    ]),
    nklib_proc:put(?MODULE, Name),
    Hds2 = nklib_util:store_value(<<"Host">>, HostHeader, Hds),
    State#state{hds=Hds2}.
