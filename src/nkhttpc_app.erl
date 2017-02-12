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

%% @doc NkHTTPC OTP Application Module
-module(nkhttpc_app).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(application).

-export([get/1, get/2, put/2, del/1]).
-export([start/0, start/2, stop/1]).

-include("nkhttpc.hrl").
-include_lib("nklib/include/nklib.hrl").


-define(APP, nkhttpc).

%% ===================================================================
%% Private
%% ===================================================================

%% @doc Starts NkDOCKER stand alone.
-spec start() -> 
    ok | {error, Reason::term()}.

start() ->
    case nklib_util:ensure_all_started(?APP, permanent) of
        {ok, _Started} ->
            ok;
        Error ->
            Error
    end.

%% @private OTP standard start callback
start(_Type, _Args) ->
    _ = code:ensure_loaded(nkhttpc_protocol),
    {ok, Pid} = nkhttpc_sup:start_link(),
    {ok, Vsn} = application:get_key(nkhttpc, vsn),
    lager:info("NkHTTPc v~s has started.", [Vsn]),
    {ok, Pid}.


%% @private OTP standard stop callback
stop(_) ->
    ok.



%% Configuration access
get(Key) ->
    nklib_config:get(?APP, Key).

get(Key, Default) ->
    nklib_config:get(?APP, Key, Default).

put(Key, Val) ->
    nklib_config:put(?APP, Key, Val).

del(Key) ->
    nklib_config:del(?APP, Key).
