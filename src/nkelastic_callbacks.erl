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
		elastic_url => binary,
		elastic_user => binary,
		elastic_pass => binary
	}.


plugin_config(Config, _Service) ->
	case Config of
		#{elastic_url:=Url} ->
			Cache = case Config of
				#{elastic_user:=User, elastic_pass:=Pass} ->
					#{url=>Url, user=>User, pass=>Pass};
				_ ->
					#{url=>Url}
			end,
			{ok, Config, Cache};
		_ ->
			{error, {missing_field, elastic_url}}
	end.


plugin_start(Config, #{id:=SrvId, config_nkelastic:=Elastic}) ->
    Spec = {
        nkelastic, 
        {nkelastic_srv, start_link, [SrvId, Elastic]},
        permanent,
        5000,
        worker,
        [nkelastic_srv]
    },
    case nkservice_srv:start_proc(SrvId, Spec) of
    	{ok, _} ->
    		{ok, Config};
    	{error, Error} ->
    		{error, {could_not_start, Error}}
    end.


plugin_stop(Config, _Service) ->
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


