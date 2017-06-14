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

%% @doc Elasticsearch worker
-module(nkelastic_worker).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(poolboy_worker).

-export([start_link/1]).


%% ===================================================================
%% Public
%% ===================================================================



%% @doc
start_link({_Id, []}) ->
    {error, no_connections};

start_link({Id, [{Conns, ConnOpts}|Rest]}) ->
    case nkhttpc_single:start_link(Id, Conns, ConnOpts) of
        {ok, Pid} ->
            {ok, Pid};
        {error, Error} when Rest == [] ->
            {error, Error};
        {error, _} ->
            start_link({Id, Rest})
    end.
