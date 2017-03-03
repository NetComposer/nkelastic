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

-module(nkelastic_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([create_service_indices/1]).


%% @private
create_service_indices(#{id:=SrvId}=Service) ->
    {ok, Map} = SrvId:elastic_get_indices(#{}, Service),
    create_service_indices(SrvId, maps:to_list(Map), Service).


%% @private
create_service_indices(_SrvId, [], _Service) ->
    ok;

create_service_indices(SrvId, [{Index, Data}|Rest], Service) ->
    case nkelastic_api:update_or_create_index(SrvId, Index, Data) of
        ok ->
            case create_service_mappings(SrvId, Index, Service) of
                ok ->
                    case create_service_aliases(SrvId, Index, Service) of
                        ok ->
                            create_service_indices(SrvId, Rest, Service);
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private
create_service_mappings(SrvId, Index, Service) ->
    {ok, Map} = SrvId:elastic_get_mappings(Index, #{}, Service),
    do_create_service_mappings(SrvId, Index, maps:to_list(Map)).


%% @private
do_create_service_mappings(_SrvId, _Index, []) ->
    ok;

do_create_service_mappings(SrvId, Index, [{Type, Data}|Rest]) ->
    case nkelastic_api:add_mapping(SrvId, Index, Type, Data) of
        ok ->
            do_create_service_mappings(SrvId, Index, Rest);
        {error, Error} ->
            {error, Error}
    end.


%% @private
create_service_aliases(SrvId, Index, Service) ->
    {ok, Map} = SrvId:elastic_get_aliases(Index, #{}, Service),
    do_create_service_aliases(SrvId, Index, maps:to_list(Map)).


%% @private
do_create_service_aliases(_SrvId, _Index, []) ->
    ok;

do_create_service_aliases(SrvId, Index, [{Name, Data}|Rest]) ->
    case nkelastic_api:add_alias(SrvId, Index, Name, Data) of
        ok ->
            do_create_service_aliases(SrvId, Index, Rest);
        {error, Error} ->
            {error, Error}
    end.