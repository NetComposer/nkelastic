%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @doc 
-module(nkelastic_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-define(SRV, es_test).

-compile(export_all).

-include_lib("nkservice/include/nkservice.hrl").

%% ===================================================================
%% Public
%% ===================================================================


%% @doc Starts the service
start() ->
    {ok, List} = application:get_env(nkelastic, stores),
    Spec = #{
        callback => ?MODULE,
        nkelastic => List
        % debug => [{nkelastic, [full]}]
    },
    nkservice:start(?SRV, Spec).


%% @doc Stops the service
stop() ->
    nkservice:stop(?SRV).


plugin_deps() ->
    [nkelastic].



health(Pool, Num) ->
    Refs = [make_ref() || _ <- lists:seq(1,Num)],
    Self = self(),
    Start = nklib_util:m_timestamp(),
    lists:foreach(
        fun(Ref) ->
            spawn_link(
                fun() ->
                    {ok, _, Time} = nkelastic_server:req(es_test, Pool, get, "/"),
                    Self ! {t, Ref, Time}
                end)
        end,
        Refs),
    Times = wait_refs(Refs, []),
    {nklib_util:m_timestamp() - Start, Times}.



wait_refs([], Times) ->
    Times;

wait_refs(Refs, Times) ->
    receive
        {t, Ref, Time} ->
            true = lists:member(Ref, Refs),
            wait_refs(Refs -- [Ref], [Time*1000|Times])
    after 5000 ->
        error
    end.