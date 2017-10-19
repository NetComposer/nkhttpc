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

%% @doc NkDOCKER Management Server
-module(nkhttpc).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([start_link/1, start/1, stop/1, req/5, data/3, stop/2, get_all/0]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, 
         handle_info/2]).
-export_type([conn_opts/0, req_opts/0]).

-include("nkhttpc.hrl").
-include_lib("nkpacket/include/nkpacket.hrl").


-type conn_opts() ::
    #{  
        host => string() | binary(),    % Default "127.0.0.1"
        port => inet:port_number(),     % Default 80
        proto => tcp | tls,             % Default tcp
        timeout => integer(),
        headers => [{binary(), binary()}],
        auth => {basic, binary(), binary()},
        timeout => integer(),
        debug => boolean(),
        packet_debug => boolean()
    }
    | nkpacket:tls_types().


-type req_opts() :: 
    #{
        async => boolean(),
        exclusive => boolean(),
        headers => [{binary(), binary()}],
        redirect => string(),           % Send to file
        use_chunks => boolean()         % Send data in chunks
    }.


-define(TIMEOUT, 20000).


-define(DEBUG(Txt, Args),
    case get(nkhttpc_debug) of
        true -> ?LLOG(debug, Txt, Args);
        _ -> ok
    end).

-define(LLOG(Type, Txt, Args), lager:Type("NkHTTPc "++Txt, Args)).


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Starts a docker connection
-spec start_link(conn_opts()) ->
    {ok, pid()} | {error, term()}.

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).


%% @doc Starts a docker connection
-spec start(conn_opts()) ->
    {ok, pid()} | {error, term()}.

start(Opts) ->
    gen_server:start(?MODULE, [Opts], []).


%% @doc Stops a docker connection
-spec stop(pid()) ->
    ok.

stop(Pid) ->
    gen_server:cast(Pid, stop).


%% @doc Sends a message
%% Every connection has a timeout, so it will never block
-spec req(pid(), binary(), binary(), binary()|iolist()|map(), req_opts()) ->
    ok | {ok, term()|binary()} | {error, term()}.

req(Pid, Method, Path, Body, Opts) ->
    Timeout = max(maps:get(timeout, Opts, ?TIMEOUT), ?TIMEOUT),
    case nklib_util:call(Pid, {req, Method, Path, Body, Opts}, Timeout) of
        {error, {exit, {{timeout, _}, _}}} -> 
            {error, call_timeout};
        Other -> 
            Other
    end.


%% @doc Sends in-message data
-spec data(pid(), reference(), iolist()) ->
    ok | {error, term()}.

data(Pid, Ref, Data) ->
    nklib_util:call(Pid, {data, Ref, Data}).


%% @doc Finished an asynchronous command
-spec stop(pid(), reference()) ->
    ok | {error, term()}.

stop(Pid, Ref) ->
    nklib_util:call(Pid, {stop, Ref}).


get_all() ->
    nklib_proc:values(?MODULE).



%% ===================================================================
%% gen_server
%% ===================================================================


-record(req, {
    ref :: reference(),
    pid :: pid(), 
    mon :: reference(),
    method :: binary(),
    path :: binary(),
    hds :: [{binary(), binary()}],
    body :: map() | list() | binary(),
    conn_pid :: pid(),
    conn_mon :: pid(),
    exclusive = false :: boolean(),
    async = false :: boolean(),
    redirect = undefined :: file:io_device(),
    use_chunks = false :: boolean(),
    status = 0 :: integer(),
    is_stream = false :: boolean(),
    % ct :: json | stream | undefined, 
    chunks = [] :: [term()],
    stream_buff = <<>> :: binary()
}).

-record(state, {
    conns :: [nkpacket:netspec()],
    conn_opts :: map(),
    hds :: [{binary(), binary()}],
    pool_size :: integer(),
    pool :: [pid()],
    reqs :: [#req{}]
}).


%% @private 
-spec init(term()) ->
    {ok, #state{}} | {stop, term()}.

init([#{host:=Host}=Opts]) ->
    % process_flag(trap_exit, true),      %% Allow calls to terminate/2
    nklib_proc:put(?MODULE, Host),
    Debug = maps:get(debug, Opts, false),
    put(nkhttpc_debug, Debug),
    case nkpacket_dns:ips(Host) of
        [] ->
            {stop, no_destination};
        Ips ->
            Port = maps:get(port, Opts, 80),
            Proto = maps:get(proto, Opts, tcp),
            Conns = [{nkhttpc_protocol, Proto, Ip, Port} || Ip <- Ips],
            TLSKeys = nkpacket_util:tls_keys(),
            TLSOpts = maps:with(TLSKeys, Opts),
            ConnOpts = TLSOpts#{
                class => {nkhttpc, self()},
                monitor => self(), 
                user => {notify, self()}, 
                idle_timeout => maps:get(timeout, Opts, ?TIMEOUT),
                debug => maps:get(packet_debug, Opts, false)
            },
            HostHd = list_to_binary([
                nklib_util:to_binary(Host),
                <<":">>,
                nklib_util:to_binary(Port)
            ]),
            Hds1 = case maps:get(headers, Opts, []) of
                [] ->
                    [
                        {<<"Host">>, HostHd},
                        {<<"User-Agent">>, <<"nkhttpc">>},
                        {<<"Accept">>, <<"*/*">>}
                    ];
                UserHeaders ->
                    UserHeaders
            end,
            Hds2 = case Opts of
                #{auth:={basic, User, Pass}} ->
                    [make_basic_auth(User, Pass)|Hds1];
                _ ->
                    Hds1
            end,
            ?DEBUG("starting server to ~s (~p ~p)", [HostHd, Ips, self()]),
            State = #state{
                conns = Conns,
                conn_opts = ConnOpts,
                hds = Hds2,
                pool_size = maps:get(pool_size, Opts, 1),
                pool = [],
                reqs = []
            },
            {ok, State}
    end.



%% @private
-spec handle_call(term(), {pid(), reference()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} | {stop, term(), term(), #state{}}.

handle_call({req, Method, Path, Body, Opts}, From, State) ->
    case make_req(Method, Path, Body, From, Opts, State) of
        {ok, Req} ->
            send(Req, State);
        {error, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call({data, Ref, Data}, _From, State) ->
    case req_find(Ref, State) of
        #req{async=true, conn_pid=ConnPid}=Req ->
            case catch nkpacket_connection:send(ConnPid, {data, Ref, Data}) of
                ok -> 
                    {reply, ok, State};
                Error ->
                    State2 = stop_req({error, send_error}, Req, State),
                    {reply, {error, Error}, State2}
            end;
        false ->
            {reply, {error, unknown_ref}, State}
    end;

handle_call({stop, Ref}, _From, State) ->
    case req_find(Ref, State) of
        #req{async=true}=Req ->
            State2 = stop_req({error, user_stop}, Req, State),
            {reply, ok, State2};
        _ ->
            {reply, {error, unknown_ref}, State}
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

handle_info({nkhttpc, Ref, {head, Status, Hds}}, State) ->
    ?DEBUG("head: ~p, ~p", [Status, Hds]),
    case req_find(Ref, State) of
        #req{}=Req  ->
            Stream = case nklib_util:get_value(<<"content-type">>, Hds) of
                <<"application/vnd.docker.raw-stream">> -> true;
                _ -> false
            end,
            Req2 = Req#req{status=Status, hds=Hds, is_stream=Stream},
            {noreply, req_store(Req2, State)};
        false ->
            ?LLOG(warning, "Received unexpected head!", []),
            {noreply, State}
    end;

handle_info({nkhttpc, Ref, {chunk, Data}}, State) ->
    % ?DEBUG("chunk: ~p", [Data]),
    case req_find(Ref, State) of
        #req{is_stream=true}=Req ->
            case parse_stream(Data, Req, State) of
                {ok, Stream, Req2, State2} ->
                    {norepy, parse_chunk(Stream, Req2, State2)};
                {more, State2} ->
                    {noreply, State2}
            end;
        #req{}=Req ->
            {noreply, parse_chunk(Data, Req, State)};
        false ->
            ?LLOG(warning, "Received unexpected chunk!", []),
            {noreply, State}
    end;

handle_info({nkhttpc, Ref, {body, Body}}, State) ->
    ?DEBUG("body: ~p", [Body]),
    case req_find(Ref, State) of
        #req{}=Req ->
            case parse_body(Body, Req) of
                {ok, Req2} ->
                    {noreply, stop_req(ok, Req2, State)};
                {error, Error} ->
                    {noreply, stop_req({error, Error}, Req, State)}
            end;
        false ->
            ?LLOG(warning, "Received unexpected body!", []),
            {noreply, State}
    end;

handle_info({'DOWN', _MRef, process, MPid, _Reason}, #state{reqs=Reqs}=State) ->
    #state{pool=Pool, reqs=Reqs} = State,
    State2 = case lists:member(MPid, Pool) of
        true -> 
            ?DEBUG("connection stopped from pool (~p)", [MPid]),
            State#state{pool=Pool--[MPid]};
        false -> 
            State
    end,
    State3 = case lists:keyfind(MPid, #req.conn_pid, Reqs) of
        #req{is_stream=true}=Req ->
            case parse_body(<<>>, Req) of
                {ok, Req2} ->
                    stop_req(ok, Req2, State2);
                {error, Error} ->
                    stop_req({error, Error}, Req, State2)
            end;
        #req{}=Req ->
            stop_req({error, connection_failed}, Req, State2);
        false ->
            case lists:keyfind(MPid, #req.pid, Reqs) of
                #req{}=Req ->
                    stop_req(ignore, Req, State2);
                false ->
                    State2
            end
    end,
    {noreply, State3};

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

terminate(_Reason, _State) ->  
    ?DEBUG("terminate (~p)", [self()]),
    ok.



%% ===================================================================
%% Private
%% ===================================================================

%% @private
make_req(Method, Path, Body, {Pid, Ref}, Opts, #state{hds=Hds1}) ->
    Async = maps:get(async, Opts, false),
    Req1 = #req{
        ref = Ref,
        pid = Pid,
        method = nklib_util:to_upper(Method),
        path = nklib_util:to_binary(Path),
        hds = Hds1 ++ maps:get(headers, Opts, []),
        body = Body,
        exclusive = maps:get(exclusive, Opts, false),
        async = Async,
        use_chunks = maps:get(use_chunks, Opts, false),
        status = 0,
        is_stream = false,
        chunks = [],
        stream_buff = <<>>
    },
    Req2 = case Async of
        true ->
            Mon = erlang:monitor(process, Pid),
            Req1#req{mon=Mon};
        false ->
            Req1
    end,
    case maps:find(redirect, Opts) of
        {ok, File} ->
            case file:open(nklib_util:to_list(File), [write, raw]) of
                {ok, Port} -> 
                    {ok, Req2#req{redirect=Port}};
                {error, FileError} -> 
                    {error, {file_error, FileError}}
            end;
        error ->
            {ok, Req2}
    end.


%% @private
-spec send(#req{}, #state{}) ->
    {reply, term(), #state{}} | {noreply, #state{}}.

send(Req, State) ->
    case get_conn(Req, State) of
        {ok, Req2, State2} ->
            #req{
                ref = Ref, 
                conn_pid = ConnPid, 
                method = Method, 
                path = Path, 
                hds = Hds,
                body = Body, 
                async = Async
            } = Req2,
            Msg = {http, Ref, Method, Path, Hds, Body},
            ?DEBUG("send ~p", [Msg]),
            case nkpacket:send(ConnPid, Msg) of
                {ok, _} when Async ->
                    {reply, {ok, Ref}, req_store(Req2, State2)};
                {ok, _} ->
                    {noreply, req_store(Req2, State2)};
                {error, Error} ->
                    {reply, {error, Error}, State2}
            end;
        {error, Error} ->
            {reply, {error, Error}, State}
    end.


%% @private
get_conn(#req{exclusive=true}=Req, State) ->
    #state{conns=Conns, conn_opts=Opts, hds=Hds} = State, 
    case nkpacket:connect(Conns, Opts) of
        {ok, Pid} ->
            ?DEBUG("exclusive connection started (~p)", [Pid]),
            Mon = erlang:monitor(process, Pid),
            Req2 = Req#req{conn_pid=Pid, conn_mon=Mon, hds=Hds},
            {ok, Req2, State};
        {error, Error} ->
            {error, Error}
    end;

get_conn(Req, #state{pool_size=Size, pool=Pool}=State) ->
    #state{conns=Conns, conn_opts=Opts, hds=Hds} = State, 
    Hds2 = [{<<"Connection">>, <<"keep-alive">>} | Hds],
    case length(Pool) of
        PoolSize when PoolSize < Size ->
            case nkpacket:connect(Conns, Opts) of
                {ok, Pid} ->
                    ?DEBUG("pool connection started (~p/~p, ~p)", 
                           [PoolSize+1, Size, Pid]),
                    Mon = erlang:monitor(process, Pid),
                    Req2 = Req#req{conn_pid=Pid, conn_mon=Mon, hds=Hds2},
                    {ok, Req2, State#state{pool=Pool++[Pid]}};
                {error, Error} ->
                    {error, Error}
            end;
        _ ->
            [Pid|Rest] = Pool,
            Mon = erlang:monitor(process, Pid),
            ?DEBUG("reusing pool connection (~p)", [Pid]),
            Req2 = Req#req{conn_pid=Pid, conn_mon=Mon, hds=Hds2},
            {ok, Req2, State#state{pool=Rest++[Pid]}}
    end.


%% @private
-spec parse_chunk(binary(), #req{}, #state{}) ->
    #state{}.

parse_chunk(Data, #req{redirect=Port}=Req, State) when is_port(Port) ->
    case file:write(Port, Data) of
        ok ->
            State;
        {error, Error} -> 
            stop_req({error, {file_error, Error}}, Req, State)
    end;

parse_chunk(Data, #req{async=true, use_chunks=true}=Req, State) ->
    #req{ref=Ref, pid=Pid} = Req,
    Pid ! {nkhttpc, Ref, {data, Data}},
    State;

parse_chunk(Data, Req, #req{chunks=Chunks}=State) ->
    Req2 = Req#req{chunks=[Data|Chunks]},
    req_store(Req2, State).


%% @private
-spec parse_body(binary(), #req{}) ->
    {ok, #req{}} | {error, term()}.

parse_body(Body, #req{redirect=Port}=Req) when is_port(Port) ->
    case file:write(Port, Body) of
        ok ->
            {ok, Req};
        {error, Error} -> 
            {error, {file_error, Error}}
    end;

parse_body(Body, #req{chunks=Chunks, use_chunks=UseChunks}=Req) ->
    case Chunks of
        [] when Body == <<>> -> 
            {ok, Req#req{body = <<>>}};
        [] ->
            {ok, Req#req{body=Body}};
        _ when Body == <<>>, UseChunks == false ->
            BigBody = list_to_binary(lists:reverse(Chunks)),
            {ok, Req#req{body=BigBody}};
        _ when Body == <<>>, UseChunks == true ->
            {ok, Req#req{body=lists:reverse(Chunks)}};
        _ ->
            {error, {invalid_chunked, Chunks, Body}}
    end.



% %% @private
% -spec parse_body(binary(), #req{}, #state{}) ->
%     #state{}.

% parse_body(Body, #req{redirect=Port}=Req, State) when is_port(Port) ->
%     case file:write(Port, Body) of
%         ok ->
%             stop_req(Req, ok, State);
%         {error, Error} -> 
%             stop_req(Req, {error, {file_error, Error}}, State)
%     end;

% parse_body(Body, #req{chunks=Chunks, use_chunks=UseChunks}=Req, State) ->
%     Reply = case Chunks of
%         [] when Body == <<>> -> 
%             {ok, <<>>};
%         [] ->
%             {ok, decode(Body, Req)};
%         _ when Body == <<>>, UseChunks == false ->
%             BigBody = list_to_binary(lists:reverse(Chunks)),
%             {ok, decode(BigBody, Req)};
%         _ when Body == <<>>, UseChunks == true ->
%             {ok, lists:reverse(Chunks)};
%         _ ->
%             {ok, {invalid_chunked, Chunks, Body}}
%     end,
%     stop_req(Req, Reply, State).


%% @private
-spec parse_stream(binary(), #req{}, #state{}) ->
    {ok, binary(), #req{}, #state{}} | {more, #state{}}.

parse_stream(Data, Req, State) ->
    case do_parse_stream(Data, Req) of
        {ok, Stream, Req2} ->
            {ok, Stream, Req2, req_store(Req2, State)};
        {more, Req2} ->
            {more, req_store(Req2, State)}
    end.



%% @private
-spec do_parse_stream(binary(), #req{}) ->
    {ok, binary(), #req{}} | {more, #req{}}.

do_parse_stream(Data, #req{stream_buff=Buff}=Req) ->
    Data1 = << Buff/binary, Data/binary>>,
    case Data1 of
        _ when byte_size(Data1) < 8 ->
            {more, Req#req{stream_buff=Data1}};
        <<T, 0, 0, 0, Size:32, Msg/binary>> when T==0; T==1; T==2 ->
            D = case T of 0 -> <<"0:">>; 1 -> <<"1:">>; 2 -> <<"2:">> end,
            case byte_size(Msg) of
                Size -> 
                    {ok, <<D/binary, Msg/binary>>, Req#req{stream_buff= <<>>}};
                BinSize when BinSize < Size -> 
                    {more, Req#req{stream_buff=Data1}};
                _ -> 
                    {Msg1, Rest} = erlang:split_binary(Msg, Size),
                    {ok, <<D/binary, Msg1/binary>>, Req#req{stream_buff=Rest}}
            end;
        _ ->
            {ok, Data1, Req#req{stream_buff= <<>>}}
    end.


%% @private
-spec stop_req(term(), #req{}, #state{}) ->
    #state{}.

stop_req(Reason, Req, #state{reqs=Reqs}=State) ->
    case Req of
        #req{async=true, mon=UserMon} ->
            erlang:demonitor(UserMon);
        _ ->
            ok
    end,
    case Req of
        #req{exclusive=true, conn_pid=ConnPid, conn_mon=ConnMon} ->
            erlang:demonitor(ConnMon),
            ?DEBUG("stop exclusive connection (~p)", [ConnPid]),
            nkpacket_connection:stop(ConnPid, normal);
        _ ->
            ok
    end,
    case Req of
        #req{redirect=Port} when is_port(Port) ->
            file:close(Port);
        _ ->
            ok
    end,
    #req{ref=Ref, pid=Pid, status=Status, hds=Hds, body=Body} = Req,
    Reply = case Reason of
        ok ->
            {ok, Status, Hds, Body};
        ignore ->
            ignore;
        {error, Error} ->
            {error, Error}
    end,
    case Req of
        _ when Reply == ignore ->
            ok;
        #req{async=true} ->
            Pid ! {nkhttpc, Ref, Reply};
        #req{} ->
            gen_server:reply({Pid, Ref}, Reply)
    end,
    Reqs2 = lists:keydelete(Ref, #req.ref, Reqs),
    State#state{reqs=Reqs2}.


%% @private
req_find(Ref, #state{reqs=Reqs}) ->
    lists:keyfind(Ref, #req.ref, Reqs).


%% @private
req_store(#req{ref=Ref}=Req, #state{reqs=Reqs}=State) ->
    Reqs2 = lists:keystore(Ref, #req.ref, Reqs, Req),
    State#state{reqs=Reqs2}.


%% @private
make_basic_auth(User, Pass) ->
    Auth = base64:encode(list_to_binary([User, ":", Pass])),
    {<<"Authorization">>, <<"Basic ", Auth/binary>>}.



