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

%% @doc NkELASTIC Plugin Config

-module(nkelastic_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([plugin_deps/0, plugin_api/1, plugin_config/3,
         plugin_start/4, plugin_update/5]).

-include("nkelastic.hrl").

-define(LLOG(Type, Txt, Args),lager:Type("NkELASTIC "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% Plugin Callbacks
%% ===================================================================


%% @doc
plugin_deps() ->
    [].

%% @doc
plugin_api(?PKG_ELASTIC) ->
    #{
        luerl => #{
            request => {nkelastic, luerl_request}
        }
    };

plugin_api(_Class) ->
    #{}.


%% @doc
plugin_config(?PKG_ELASTIC, #{id:=Id, config:=Config}=Spec, #{id:=SrvId}) ->
    Syntax = #{
        targets => {list, #{
            url => binary,
            opts => nkpacket_syntax:safe_syntax(),
            weight => {integer, 1, 1000},
            pool => {integer, 1, 1000},
            refresh => boolean,
            headers => map,
            '__mandatory' => [url]
        }},
        debug => {list, {atom, [basic, full, pooler]}},
        index => binary,                        % Default index
        type => binary,                         % Default type
        resolveInterval => {integer, 0, none},
        '__mandatory' => [targets]
    },
    case nklib_syntax:parse(Config, Syntax) of
        {ok, Parsed, _} ->
            CacheMap1 = nkservice_config_util:get_cache_map(Spec),
            Opts1 = lists:flatten([
                {srv_id, SrvId},
                {package_id, Id},
                case maps:get(index, Parsed, <<>>) of
                    <<>> -> [];
                    Index -> {index, Index}
                end,
                case maps:get(type, Parsed, <<>>) of
                    <<>> -> [];
                    Type -> {type, Type}
                end
            ]),
            Opts2 = maps:from_list(Opts1),
            CacheMap2 = nkservice_config_util:set_cache_key(nkelastic, Id, opts, Opts2, CacheMap1),
            Spec2 = nkservice_config_util:set_cache_map(CacheMap2, Spec),
            DebugMap1 = nkservice_config_util:get_debug_map(Spec2),
            Debug1 = maps:get(debug, Parsed, []),
            Debug2 = case lists:member(full, Debug1) of
                true ->
                    full;
                false ->
                    lists:member(basic, Debug1)
            end,
            DebugMap2 = nkservice_config_util:set_debug_key(nkelastic, Id, debug, Debug2, DebugMap1),
            Spec3 = nkservice_config_util:set_debug_map(DebugMap2, Spec2),
            {ok, Spec3#{config:=Parsed}};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(_Class, _Package, _Service) ->
    continue.


%% @doc
plugin_start(?PKG_ELASTIC, #{id:=Id, config:=Config}, Pid, Service) ->
    insert(Id, Config, Pid, Service);

plugin_start(_Id, _Spec, _Pid, _Service) ->
    continue.


%% @doc
%% Even if we are called only with modified config, we check if the spec is new
plugin_update(?PKG_ELASTIC, #{id:=Id, config:=NewConfig}, OldSpec, Pid, Service) ->
    case OldSpec of
        #{config:=NewConfig} ->
            ok;
        _ ->
            insert(Id, NewConfig, Pid, Service)
    end;

plugin_update(_Class, _NewSpec, _OldSpec, _Pid, _Service) ->
    ok.



%% ===================================================================
%% Internal
%% ===================================================================


%% @private
insert(Id, Config, SupPid, #{id:=SrvId}) ->
    Debug = maps:get(debug, Config, []),
    PoolConfig = #{
        targets => maps:get(targets, Config),
        debug => lists:member(pooler, Debug),
        resolve_interval => maps:get(resolveInterval, Config, 0)
    },
    Spec = #{
        id => Id,
        start => {nkpacket_httpc_pool, start_link, [{SrvId, Id}, PoolConfig]}
    },
    case nkservice_packages_sup:update_child(SupPid, Spec, #{}) of
        {ok, ChildPid} ->
            nklib_proc:put({nkelastic, SrvId, Id}, undefined, ChildPid),
            ?LLOG(debug, "started ~s (~p)", [Id, ChildPid]),
            ok;
        not_updated ->
            ?LLOG(debug, "didn't upgrade ~s", [Id]),
            ok;
        {upgraded, ChildPid} ->
            nklib_proc:put({nkelastic, SrvId, Id}, undefined, ChildPid),
            ?LLOG(info, "upgraded ~s (~p)", [Id, ChildPid]),
            ok;
        {error, Error} ->
            ?LLOG(notice, "start/update error ~s: ~p", [Id, Error]),
            {error, Error}
    end.

