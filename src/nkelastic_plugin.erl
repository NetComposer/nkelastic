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

%% @doc NkELASTIC callbacks

-module(nkelastic_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([plugin_deps/0, plugin_syntax/0, plugin_config/2,plugin_start/2]).



%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkELASTIC is started as a NkSERVICE plugin
%% ===================================================================


plugin_deps() ->
    [].


plugin_syntax() ->
	#{
	    nkelastic_pools =>
            {list, #{
                id => binary,
                url => fun nkelastic_util:parse_url/1,
                pool_size => {integer, 1, none},
                pool_overflow => {integer, 1, none},
                '__mandatory' => [id, url]
           }}
}.


plugin_config(#{nkelastic_pools:=Pools}=Config, #{id:=SrvId}) ->
    case nkservice_util:get_config_ids(Pools) of
        {ok, _} ->
            ServerId = nklib_util:to_atom(<<(nklib_util:to_binary(SrvId))/binary, "_nkelastic">>),
            {ok, Config#{nkelastic_server_id=>ServerId}, ServerId};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(Config, _Service) ->
    {ok, Config}.


plugin_start(#{nkelastic_server_id:=ServerId, nkelastic_pools:=Pools}=Config, #{id:=SrvId}) ->
    {ok, _} = nkservice_srv:start_proc(SrvId, ServerId, nkelastic_server, [SrvId, ServerId, Pools]),
    {ok, Config};

plugin_start(Config, _Service) ->
    {ok, Config}.


