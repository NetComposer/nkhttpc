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

-export([start_link/3, start/3, stop/1, req/6, get_all/0]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, 
         handle_info/2]).
-export_type([conn/0, conn_opts/0]).

-include("nkhttpc.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").



-type conn() :: {tcp|tls, inet:ip_address(), inet:port_number()}.

-type conn_opts() ::
    #{  
        ?TLS_TYPES,
        host => binary(),
        headers => [{binary(), binary()}],
        auth => {basic, binary(), binary()},
        idle_timeout => integer(),
        debug => boolean(),
        packet_debug => boolean()
    }.


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
-spec start_link(term(), [conn()], conn_opts()) ->
    {ok, pid()} | {error, term()}.

start_link(Id, Conns, Opts) ->
    gen_server:start_link(?MODULE, [Id, Conns, Opts], []).


%% @doc Starts a http connection
-spec start(term(), [conn()], conn_opts()) ->
    {ok, pid()} | {error, term()}.

start(Id, Conns, Opts) ->
    gen_server:start(?MODULE, [Id, Conns, Opts], []).


%% @doc Stops a connection
-spec stop(pid()) ->
    ok.

stop(Pid) ->
    gen_server:cast(Pid, stop).


%% @doc Sends a message
%% Every connection has a timeout, so it will never block
-spec req(pid(), binary(), binary(), [{binary(), binary()}], binary()|iolist(), integer()) ->
    {ok, integer(), [{binary(), binary()}], binary(), integer()} | {error, term()}.

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
    opts :: map(),
    conns :: list(),
    conn_opts :: map(),
    host :: binary(),
    hds :: [{binary(), binary()}],
    conn_pid :: pid(),
    req = undefined :: #req{},
    debug :: boolean()
}).


%% @private 
-spec init(term()) ->
    {ok, #state{}} | {stop, term()}.

init([Id, Conns, Opts]) ->
    nklib_proc:put(?MODULE, Id),
    TLSKeys = nkpacket_util:tls_keys(),
    TLSOpts = maps:with(TLSKeys, Opts),
    ConnOpts = TLSOpts#{
        class => {nkhttpc, self()},
        monitor => self(),
        user => {notify, self()},
        idle_timeout => maps:get(idle_timeout, Opts, ?IDLE_TIMEOUT),
        debug => maps:get(packet_debug, Opts, false)
    },
    State = #state{
        id = Id,
        opts = Opts,
        conns = Conns,
        conn_opts = ConnOpts,
        debug = maps:get(debug, Opts, false)
    },
    case connect(State) of
        {ok, Ip, State2} ->
            ?LLOG(info, "new connection (~s)", [nklib_util:to_host(Ip)], State),
            {ok, State2};
        {error, Error} ->
            {stop, Error}
end.


%% @private
-spec handle_call(term(), {pid(), reference()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} | {stop, term(), term(), #state{}}.

handle_call({req, Method, Path, Hds, Body}, From, #state{req=undefined}=State) ->
    #state{hds=BaseHds, conn_pid=ConnPid} = State,
    Ref = make_ref(),
    Msg = {http, Ref, Method, Path, BaseHds++Hds, Body},
    ?DEBUG("send ~p", [Msg], State),
    case nkpacket:send(ConnPid, Msg) of
        {ok, _} ->
            Req = #req{
                ref = Ref,
                from = From
            },
            {noreply, State#state{req=Req}};
        {error, Error} ->
            {error, Error, State}
    end;

handle_call({req, _Method, _Path, _Body, _Opts}, _From, State) ->
    {reply, {error, request_not_finished}, State};

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

handle_info({nkhttpc, Ref, {head, Status, Hds}}, #state{req=Req}=State) ->
    ?DEBUG("head: ~p, ~p", [Status, Hds], State),
    #req{ref=Ref} = Req,
    Req2 = Req#req{status=Status, hds=Hds},
    {noreply, State#state{req=Req2}};

handle_info({nkhttpc, Ref, {chunk, Data}}, #state{req=Req}=State) ->
    % ?DEBUG("chunk: ~p", [Data]),
    #req{ref=Ref, chunks=Chunks} = Req,
    Req2 = Req#req{chunks=[Data|Chunks]},
    {noreply, State#state{req=Req2}};

handle_info({nkhttpc, Ref, {body, Body}}, #state{req=Req}=State) ->
    ?DEBUG("body: ~p", [Body], State),
    #req{ref=Ref, status=Status, hds=Hds, from=From, chunks=Chunks} = Req,
    Body2 = case Chunks of
        [] ->
            Body;
        _ when Body == <<>> ->
            list_to_binary(lists:reverse(Chunks));
        _ ->
            <<"invalid_chunked">>
    end,
    gen_server:reply(From, {ok, Status, Hds, Body2}),
    {noreply, State#state{req=undefined}};

handle_info({'DOWN', _MRef, process, Pid, Reason}, #state{conn_pid=Pid}=State) ->
    case connect(State) of
        {ok, Ip, State2} ->
            lager:warning("NKLOG Reconnected"),
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
connect(#state{conns=Conns, conn_opts=ConnOpts}=State) ->
    case connect(nklib_util:randomize(Conns), ConnOpts) of
        {ok, {_Proto, Ip, Port}, Pid} ->
            monitor(process, Pid),
            State2 = get_headers(Ip, Port, State),
            State3 = State2#state{conn_pid = Pid},
            {ok, Ip, State3};
        {error, Error} ->
            {error, Error}
    end.


%% @private
connect([], _ConnOpts) ->
    {error, no_connections};

connect([{Proto, Ip, Port}|Rest], ConnOpts) ->
    Conn = {nkhttpc_protocol, Proto, Ip, Port},
    case nkpacket:connect(Conn, ConnOpts) of
        {ok, Pid} ->
            {ok, {Proto, Ip, Port}, Pid};
        {error, Error} when Rest==[] ->
            {error, Error};
        {error, _Error} ->
            connect(Rest, ConnOpts)
    end.


%% @private
get_headers(Ip, Port, #state{opts=Opts}=State) ->
    Host = maps:get(host, Opts, Ip),
    HostHeader = list_to_binary([
        nklib_util:to_host(Host),
        <<":">>,
        nklib_util:to_binary(Port)
    ]),
    nklib_proc:put(?MODULE, Host),
    Hds1 = case maps:get(headers, Opts, []) of
       [] ->
           [
               {<<"Host">>, HostHeader},
               {<<"Connection">>, <<"keep-alive">>},
               {<<"Accept">>, <<"*/*">>}
           ];
       UserHeaders ->
           UserHeaders
    end,
    Hds2 = case Opts of
       #{auth:={basic, User, Pass}} ->
           Auth = base64:encode(list_to_binary([User, ":", Pass])),
           [{<<"Authorization">>, <<"Basic ", Auth/binary>>} | Hds1];
       _ ->
           Hds1
    end,
    State#state{host=HostHeader, hds=Hds2}.
