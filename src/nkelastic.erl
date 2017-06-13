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

%% @doc NkELASTIC application

-module(nkelastic).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export_type([search_spec/0]).

-export([t/0]).

%% ===================================================================
%% Types
%% ===================================================================

-type search_spec() :: nkelastic_search:search_spec().


t() ->
    {ok, List} = application:get_env(nkelastic, stores),
    Syntax = nkelastic_callbacks:plugin_syntax(),
    {ok, #{nkelastic:=L}, []} = nklib_syntax:parse(#{nkelastic=>List}, Syntax),
    nkelastic_callbacks:parse_stores(L, #{}).