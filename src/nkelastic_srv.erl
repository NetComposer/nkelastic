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

-module(nkelastic_srv).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([request/3, request/4]).
-export([get_all/1, get_all/0]).
-export([start_link/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
         handle_cast/2, handle_info/2]).


%% Set debug options for service:
%% debug => [nkelastic] or [{nkelastic, full}]

-define(DEBUG(Txt, Args, State),
    case State#state.debug of
        {true, _} -> ?LLOG(debug, Txt, Args, State);
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
                {true, _} ->
    				?LLOG(debug, "error ~s (~s): ~p", [Type, Reason, Error]);
    			_ ->
    				ok
    		end,
    		{error, get_error(Type, Reason)};
        {error, {http_code, 404, _}, _Debug} ->
        	{error, object_not_found};
        {error, {http_code, 400, R}, _Debug} ->
            ?LLOG(warning, "invalid request: ~p", [R]),
            {error, internal_error};
        {error, Error, _Debug} ->
    		?LLOG(warning, "unrecognized error: ~p", [Error]),
            {error, elastic_error}
    end.


%% @private
get_error(<<"index_not_found_exception">>, _Reason) ->
    index_not_found;

get_error(<<"search_phase_execution_exception">>, _Reason) ->
    search_error;

get_error(Type, Reason) ->
    lager:notice("ES error: ~s, ~s", [Type, Reason]),
    {es_error, Type}.


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
    debug :: {boolean(), Full::boolean()}
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
				<<"number_of_data_nodes">> := _Nodes,
				<<"status">> := Status
			} = Data,
            case Status of
                <<"green">> ->
                    % ?LLOG(info, "Cluster OK", []),
                    ok;
                _ ->
%%                    ?LLOG(warning, "cluster status: ~s (~p nodes)", [Status, Nodes]),
                    ok
            end;
        {error, Error, _Debug} ->
            ?LLOG(warning, "error contacting backend: ~p", [Error])
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
    nkservice_util:register_for_changes(SrvId),
    Debug = case nkservice_util:get_debug_info(SrvId, nkelastic) of
        {true, full} -> {true, true};
        {true, _} -> {true, false};
        _ -> false
    end,
    %% ?LLOG(warning, "debug: ~p", [Debug]),
    State#state{debug=Debug}.


%% @private
do_request(Method, Path, Body, State) ->
	#state{url=Url, headers=Headers1, debug=Debug} = State,
    {Headers2, Body2} = case is_map(Body) of
        true -> 
            {
            	[{<<"Content-Type">>, <<"application/json">>}|Headers1],
            	nklib_json:encode(Body)
            };
        false ->
        	{Headers1, Body}
    end,
    case Body2 of
        error ->
            ?LLOG(error, "Json error: ~p", [Body]),
            error(json_error);
        _ ->
            ok
    end,
    HttpOpts = [
        {connect_timeout, ?CONNECT_TIMEOUT},
        {recv_timeout, ?RECV_TIMEOUT},
        insecure,
        with_body,
        {pool, default}
        % {ssl_options, [{ciphers, Ciphers}]}
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
                    case nklib_util:get_value(<<"content-type">>, RespHds) of
                        <<"application/json", _/binary>> ->
                            nklib_json:decode(RespBody);
                        _ ->
                            RespBody
                    end
            end,
            case Path of
                "_cluster/health" ->
                	ok;
                _ when Debug=={true, true} ->
                    DbgSendBody = case is_map(Body) orelse is_list(Body) of
                        true ->
                            <<"-> ", (nklib_json:encode_pretty(Body))/binary, "\n">>;
                        false ->
                            <<>>
                    end,
                    DbgRespBody = case is_map(RespBody2) orelse is_list(RespBody2) of
                        true ->
                            <<"<- ", (nklib_json:encode_pretty(RespBody2))/binary, "\n">>;
                        false ->
                            <<"<- ", RespBody2/binary, "\n">>
                    end,
                    ?LLOG(debug, "~s ~s: ~p ~p msecs\n~s~s",
                          [Method, Path, Code, Time, DbgSendBody, DbgRespBody], State);
                _ when Debug=={true, false} ->
                    ?LLOG(debug, "~s ~s: ~p ~p msecs", [Method, Path, Code, Time], State);
                _ ->
                    ok
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









