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

-module(nkelastic_srv).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([request/3, request/4]).
-export([get_all/1, get_all/0]).
-export([start_link/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


-define(DEBUG(Txt, Args, State),
    case erlang:get(nkelastic_debug) of
        true -> ?LLOG(debug, Txt, Args, State);
        _ -> ok
    end).


-define(LLOG(Type, Txt, Args, State),
    lager:Type(
            [
                {url, State#state.url}
            ],
           "NkELASTIC (~s): "++Txt, 
           [State#state.url | Args])).

-define(LLOG(Type, Txt, Args), lager:Type("NkELASTIC "++Txt, Args)).

-define(CHECK_TIME, 10000).
-define(CALL_TIMEOUT, 30000).
-define(CONNECT_TIMEOUT, 15000).
-define(RECV_TIMEOUT, 5000).



%% ===================================================================
%% Types
%% ===================================================================

-type config() ::
	#{
		url => binary(),
		user => binary(),
		pass => binary()
	}.

-type id() :: nkservice:id().
-type method() :: get | put | post | head | delete.
-type path() :: iolist().
-type body() :: map() | binary().
-type error() :: {es_error, binary(), binary()} | term().


%% ===================================================================
%% Public
%% ===================================================================


%% @private
-spec request(id(), method(), path()) ->
	ok | {ok, term()} | {error, error()}.

request(Srv, Method, Path) ->
    request(Srv, Method, Path, <<>>).


%% @private
-spec request(id(), method(), path(), body()) ->
	{ok, term()} | {error, error()}.

request(Srv, Method, Path, Body) ->
    case do_call(Srv, {request, Method, Path, Body}) of
        {ok, Data, _Debug} ->
            {ok, Data};
        {error, {http_code, _, #{<<"error">>:=Error}}, Debug} ->
        	#{<<"type">>:=Type, <<"reason">>:=Reason} = Error,
        	case Debug of
        		true ->
    				?LLOG(debug, "error ~s (~s): ~p", [Type, Reason, Error]);
    			_ ->
    				ok
    		end,
    		{error, {es_error, Type, Reason}};
        {error, {http_code, 404, #{<<"found">>:=false}}, _Debug} ->
        	{error, {<<"obj_not_found">>, <<"Object not found">>}};
        {error, Error, _Debug} ->
    		?LLOG(warning, "unrecognized error: ~p", [Error]),
            {error, elastic_error}
    end.


%% @private
-spec get_all(nkservice:id()) ->
	[pid()].

get_all(Srv) ->
	case nkservice_srv:get_srv_id(Srv) of
		{ok, SrvId} ->
			[Pid || {_, Pid} <- nklib_proc:values({?MODULE, SrvId})];
		_ ->
			[]
	end.


%% @private
-spec get_all() ->
	[{nkservice:id(), pid()}].

get_all() ->
	nklib_proc:values(?MODULE).


%% @private
find(Pid) when is_pid(Pid) ->
	{ok, Pid};

find(Srv) ->
	case get_all(Srv) of
		[Pid|_] -> {ok, Pid};
		[] -> not_found
	end.


%% @private
do_call(Srv, Msg) ->
	case find(Srv) of
		{ok, Pid} ->
		    nklib_util:call(Pid, Msg, ?CALL_TIMEOUT);
		not_found ->
			{error, server_not_found}
	end.


% ===================================================================
%% gen_server behaviour
%% ===================================================================


%% @private
-spec start_link(nkservice:id(), config()) ->
	{ok, pid()}.

start_link(SrvId, Config) ->
    gen_server:start_link(?MODULE, [SrvId, Config], []).


-record(state, {
	srv_id :: nkservice:id(),
    url :: binary(),
    headers :: [{binary(), binary()}],
    debug :: boolean()
}).



%% @private
-spec init(term()) ->
    {ok, tuple()} | {ok, tuple(), timeout()|hibernate} |
    {stop, term()} | ignore.

init([SrvId, #{url:=Url}=Config]) ->
    Hds = case Config of
        #{user:=User, pass:=Pass} ->
            Auth = base64:encode(list_to_binary([User, ":", Pass])),
            [{<<"Authorization">>, <<"Basic ", Auth/binary>>}];
        _ ->
        	[]
    end,
    State = #state{
    	srv_id = SrvId,
        url = Url,
        headers = Hds
    },
    nklib_proc:put({?MODULE, SrvId}),
    nklib_proc:put(?MODULE, SrvId),
    self() ! check_cluster,
    {ok, set_log(State)}.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} |
    {stop, Reason::term(), #state{}} | {stop, Reason::term(), Reply::term(), #state{}}.

handle_call({request, Method, Path, Body}, From, State) ->
    spawn_link(
        fun() -> 
            Reply = do_request(Method, Path, Body, State),
            gen_server:reply(From, Reply)
        end),
    {noreply, State};

handle_call(Msg, _From, State) ->
    lager:error("Module ~p received unexpected call ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_cast(Msg, State) -> 
    lager:error("Module ~p received unexpected cast ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info(check_cluster, State) ->
    case do_request(get, "_cluster/health", <<>>, State) of
        {ok, Data, _Debug} ->
			#{
				<<"number_of_data_nodes">> := Nodes, 
				<<"status">> := Status
			} = Data,
            case Status of
                <<"green">> ->
                    % ?LLOG(info, "Cluster OK", []),
                    ok;
                _ ->
                    ?LLOG(warning, "cluster status: ~s (~p nodes)", [Status, Nodes])
            end;
        {error, Error} ->
            ?LLOG(warning, "Error contacting backend: ~p", [Error])
    end,
    erlang:send_after(?CHECK_TIME, self(), check_cluster),
    {noreply, State};

handle_info({nkservice_updated, _SrvId}, State) ->
    {noreply, set_log(State)};

handle_info(Info, State) -> 
    lager:warning("Module ~p received unexpected info: ~p (~p)", [?MODULE, Info, State]),
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
    ok.
    



% ===================================================================
%% Internal
%% ===================================================================


%% @private
set_log(#state{srv_id=SrvId}=State) ->
    Debug = case nkservice_util:get_debug_info(SrvId, nkelastic) of
        {true, _} -> true;
        _ -> false
    end,
    ?LLOG(info, "debug: ~p", [Debug]),
    put(nkelastic_debug, Debug),
    State#state{debug=Debug}.



%% @private
do_request(Method, Path, Body, State) ->
	#state{url=Url, headers=Headers1, debug=Debug} = State,
	case Debug of
		true when is_map(Body) ->
			?LLOG(debug, "~s\n~s", 
				[
                	list_to_binary([Path]),
                	nklib_json:encode_pretty(Body)
            	], 
                State);
		_ ->
			ok
	end,
    {Headers2, Body2} = case is_map(Body) of
        true -> 
            {
            	[{<<"Content-Type">>, <<"application/json">>}|Headers1],
            	nklib_json:encode(Body)
            };
        false ->
        	{Headers1, Body}
    end,
    Ciphers = ssl:cipher_suites(),
    % Hackney fails with its default set of ciphers
    % See hackney.ssl#44
    HttpOpts = [
        {connect_timeout, ?CONNECT_TIMEOUT},
        {recv_timeout, ?RECV_TIMEOUT},
        insecure,
        with_body,
        {pool, default},
        {ssl_options, [{ciphers, Ciphers}]}
    ],
    Start = nklib_util:m_timestamp(),
    Url2 = list_to_binary([Url, Path]),
    case hackney:request(Method, Url2, Headers2, Body2, HttpOpts) of
        {ok, Code, RespHds, RespBody}  ->
            Time = nklib_util:m_timestamp() - Start,
        	RespBody2 = case nklib_util:get_value(<<"Content-Type">>, RespHds) of
                <<"application/json", _/binary>> ->
                    nklib_json:decode(RespBody);
                _ ->
                    RespBody
            end,
            case Path of
                "_cluster/health" -> 
                	ok;
                _ ->
                 	?LLOG(info, "~s ~s: ~p ~p msecs", 
                 		  [Method, Path, Code, Time], State),
                	case Debug of
						true ->
							DebugBody = case is_map(RespBody2) of
								true -> nklib_json:encode_pretty(RespBody2);
								false -> RespBody
							end,
		                	?LLOG(debug, "~s ~s: ~p ~p msecs\n~s", 
		                		  [Method, Path, Code, Time, DebugBody], State);
						_ ->
							ok
		            end
            end,
            case (Code>=200 andalso  Code<300) of
            	true ->
            		{ok, RespBody2, Debug};
            	false ->
            		{error, {http_code, Code, RespBody2}, Debug}
            end;
        {error, Error} ->
            {error, Error, Debug}
    end.









