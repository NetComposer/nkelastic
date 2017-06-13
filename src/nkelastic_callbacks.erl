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

-module(nkelastic_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([elastic_get_indices/2, elastic_get_mappings/3, elastic_get_aliases/3, elastic_get_templates/2]).
-export([plugin_deps/0, plugin_syntax/0, plugin_config/2,
         plugin_start/2, plugin_stop/2, service_init/2]).
-export([error_reason/2]).
%%-export([api_server_cmd/2, api_server_syntax/4]).
-compile(export_all).


-include("nkelastic.hrl").
-include_lib("nkservice/include/nkservice.hrl").



%% ===================================================================
%% Types
%% ===================================================================

% -type continue() :: continue | {continue, list()}.




%% ===================================================================
%% Offered callbacks
%% ===================================================================

-type index_map() :: #{Index::binary() => map()}.
-type type_map() :: #{Type::binary() => map()}.
-type alias_map() :: #{Name::binary() => map()}.
-type template_map() :: #{Name::binary() => map()}.


%% @doc Will be called on plugin start to get indices to create or update
-spec elastic_get_indices(index_map(), nkservice:service()) ->
    {ok, index_map()}.

elastic_get_indices(Acc, _Service) ->
    {ok, Acc}.


%% @doc Will be called on plugin start to get mappings to create or update
-spec elastic_get_mappings(Index::binary(), type_map(), nkservice:service()) ->
    {ok, type_map()}.

elastic_get_mappings(_Index, Acc, _Service) ->
    {ok, Acc}.


%% @doc Will be called on plugin start to get aliases
-spec elastic_get_aliases(Index::binary, alias_map(), nkservice:service()) ->
    {ok, alias_map()}.

elastic_get_aliases(_Index, Acc, _Service) ->
    {ok, Acc}.


%% @doc Will be called on plugin start to get templates
-spec elastic_get_templates(template_map(), nkservice:service()) ->
    {ok, type_map()}.

elastic_get_templates(Acc, _Service) ->
    {ok, Acc}.


%% ===================================================================
%% Plugin callbacks
%%
%% These are used when NkELASTIC is started as a NkSERVICE plugin
%% ===================================================================


plugin_deps() ->
    [].


plugin_syntax() ->
	#{
	    nkelastic =>
            {list, #{
                id => binary,
                url => binary,
                pool_size => {integer, 1, none},
                pool_overflow => {integer, 1, none},
                '__mandatory' => [url]
           }}
}.


plugin_config(#{nkelastic:=List}=Config, #{id:=SrvId}) ->
    case parse_stores(List, #{}) of
        {ok, ParsedMap} ->
            ServerId = nklib_util:to_atom(<<(nklib_util:to_binary(SrvId))/binary, "_nkelastic">>),
            {ok, Config#{nkelastic_stores=>{ServerId, ParsedMap}}, ServerId};
        {error, Error} ->
            {error, Error}
    end;

plugin_config(Config, _Service) ->
    {ok, Config}.


plugin_start(#{nkelastic_stores:={ServerId, ParsedMap}}=Config, #{id:=SrvId}) ->
    {ok, _} = nkservice_srv:start_proc(SrvId, ServerId, nkelastic_server, [SrvId, ServerId, ParsedMap]),
    {ok, Config};

plugin_start(Config, _Service) ->
    {ok, Config}.


plugin_stop(Config, #{id:=_Id}) ->
    {ok, Config}.



service_init(#{id:=Id}=Service, State) ->
    case nkelastic_util:create_service_indices(Service) of
        ok ->
            {ok, State};
        {error, Error} ->
            lager:error("NkELASTIC: Could not create indices for ~p: ~p", [Id, Error]),
            {stop, nkelastic_create_indices}
    end.



%% ===================================================================
%% Error Codes
%% ===================================================================

%% @doc
-spec error_reason(nkservice:lang(), nkservice:error()) ->
	{binary(), binary()} | continue.

error_reason(_, {es_error, Code, Reason}) -> 
	{store_error, "Store error ~s: ~s", [Code, Reason]};

error_reason(_Lang, _Error) ->
	continue.



% ===================================================================
%% API Server Callbacks
%% ===================================================================

%%%% @private
%%api_server_cmd(#api_req{class=elastic, subclass=Sub, cmd=Cmd}=Req, State) ->
%%	nkelastic_api:cmd(Cmd, Sub, Req, State);
%%
%%api_server_cmd(_Req, _State) ->
%%	continue.
%%
%%
%%%% @private
%%api_server_syntax(#api_req{class=elastic, subclass=Sub, cmd=Cmd},
%%		   		  Syntax, Defaults, Mandatory) ->
%%	nkelastic_api_syntax:syntax(Cmd, Sub, Syntax, Defaults, Mandatory);
%%
%%api_server_syntax(_Req, _Syntax, _Defaults, _Mandatory) ->
%%	continue.


%% ===================================================================
%% Util
%% ===================================================================

parse_stores([], Acc) ->
    {ok, Acc};

parse_stores([#{url:=Url}=Map|Rest], Acc) ->
    case nkpacket:parse_urls(es, [http, https], Url) of
        {ok, Conns} ->
            Id = maps:get(id, Map, <<"main">>),
            case maps:is_key(Id, Acc) of
                false ->
                    parse_stores(Rest, Acc#{Id=>{Map, Conns}});
                true ->
                    {error, duplicated_id}
            end;
        {error, Error} ->
            {error, Error}
    end.

