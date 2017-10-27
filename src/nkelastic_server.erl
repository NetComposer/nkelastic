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

-module(nkelastic_server).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([req/4, req/5, req/6, get_pool/2]).
-export([transports/1, default_port/1, resolve_opts/0]).
-export([start_link/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3,
    handle_cast/2, handle_info/2]).

-include_lib("nkpacket/include/nkpacket.hrl").

-define(DEBUG, false).
-define(REFRESH, 30).
-define(CHECK_TIME, 10000).

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkELASTIC Server (~p) "++Txt, [State#state.srv_id|Args])).

-define(REQ_DBG(Txt, Args),
    lager:debug("NkELASTIC Debug "++Txt, Args)).





%% ===================================================================
%% Types
%% ===================================================================




%% ===================================================================
%% Public
%% ===================================================================


%% @doc
req(SrvId, Pool, Method, Path) ->
    req(SrvId, Pool, Method, Path, <<>>).


%% @doc
req(SrvId, Pool, Method, Path, Body) ->
    req(SrvId, Pool, Method, Path, Body, 30000).


%% @doc
req(SrvId, Pool, Method, Path, Body, Timeout) ->
    case get_pool(SrvId, to_bin(Pool)) of
        {ok, Pid} ->
            Debug = case nkservice_util:get_debug(SrvId, nkelastic) of
                undefined -> false;
                [] -> {true, false};
                [full] -> {true, true};
                full -> {true, true}
            end,
            Fun = fun(Worker) ->
                do_req(Worker, Method, to_bin(Path), Body, Timeout, Debug)
            end,
            poolboy:transaction(Pid, Fun);
        {error, Error} ->
            {error, Error}
    end.

%% @private
get_pool(SrvId, Id) ->
    Name = SrvId:config_nkelastic(),
    gen_server:call(Name, {get_pool, Id}).


%% @doc
-spec start_link(nkservice:id(), atom(), map()) ->
    {ok, pid()} | {error, term()}.

start_link(SrvId, Name, ConnMap) ->
    gen_server:start_link({local, Name}, ?MODULE, [SrvId, Name, ConnMap], []).



%% ===================================================================
%% Transport behaviour
%% ===================================================================

transports(_) ->
    [http, https].

default_port(_) ->
    9200.

resolve_opts() ->
    #{resolve_type=>listen}.





% ===================================================================
%% gen_server behaviour
%% ===================================================================

-record(state, {
    srv_id :: nkservice:id(),
    name :: atom(),
    pools = #{} :: #{Id::binary() => pid()},
    configs = [] :: [map()]
}).



%% @private
-spec init(term()) ->
    {ok, tuple()} | {ok, tuple(), timeout()|hibernate} |
    {stop, term()} | ignore.

init([SrvId, Name, Pools]) ->
    State = #state{
        srv_id = SrvId,
        name = Name,
        pools = #{},
        configs = Pools
    },
    lager:error("NKLOG ConnMAP ~p", [Pools]),
    process_flag(trap_exit, true),
    self() ! start_pools,
    {ok, State}.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} |
    {stop, Reason::term(), #state{}} | {stop, Reason::term(), Reply::term(), #state{}}.

handle_call({get_pool, Id}, _From, #state{pools=Pools}=State) ->
    case maps:find(Id, Pools) of
        {ok, Pid} ->
            {reply, {ok, Pid}, State};
        error ->
            {reply, {error, pool_not_available}, State}
    end;

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

handle_info(start_pools, #state{configs=Configs}=State) ->
    State2 = start_pools(Configs, State),
    erlang:send_after(?CHECK_TIME, self(), start_pools),
    {noreply, State2};

handle_info({'EXIT', Pid, _Reason}, #state{pools=Pools}=State) ->
    PoolList = maps:to_list(Pools),
    case lists:keytake(Pid, 2, PoolList) of
        {value, {Id, Pid}, PoolList2} ->
            ?LLOG(warning, "pool '~s' is down", [Id], State),
            {noreply, State#state{pools=maps:from_list(PoolList2)}};
        false ->
            lager:warning("Module ~p received unexpected down: ~p", [?MODULE, Pid]),
            {noreply, State}
    end;

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

%%%% @private
start_pools([], State) ->
    State;

start_pools([#{id:=Id, url:={nkelastic_conns, Conns}=Config}|Rest], #state{pools=Pools} = State) ->
    case maps:is_key(Id, Pools) of
        true ->
            start_pools(Rest, State);
        false ->
            HttpConns = get_conns(Conns, []),
            try
                case test_connect(Id, HttpConns, State) of
                    ok ->
                        Opts = maps:get(opts, Config, #{}),
                        PoolOpts = [
                            {worker_module, nkelastic_worker},
                            {size, maps:get(pool_size, Opts, 5)},
                            {max_overflow, maps:get(pool_overflow, Opts, 10)}
                        ],
                        {ok, Pid} = poolboy:start_link(PoolOpts, {Id, HttpConns}),
                        ?LLOG(notice, "started pool '~s'", [Id], State),
                        Pools2 = Pools#{Id => Pid},
                        start_pools(Rest, State#state{pools=Pools2});
                    {error, Error} ->
                        ?LLOG(warning, "could not start pool '~s': ~p", [Id, Error], State),
                        start_pools(Rest, State)
                end
            catch
                error:CError ->
                    Trace = erlang:get_stacktrace(),
                    ?LLOG(warning, "could not start pool1 '~s': ~p (~p)", [Id, CError, Trace], State),
                    start_pools(Rest, State)
            end
    end.


%% @private
get_conns([], Acc) ->
    Acc;

get_conns([Conn|Rest], Acc) ->
    #nkconn{transp=Transp, ip=Ip, port=Port, opts=ConnOpts} = Conn,
    Opts2 = maps:with([host], ConnOpts),
    Opts3 = Opts2#{
        path => nklib_parse:path(maps:get(path, ConnOpts, <<>>)),
        idle_timeout => 0,
        debug => ?DEBUG,
        packet_debug => false,
        refresh_interval => ?REFRESH,
        refresh_request => {get, <<"/">>, [], <<>>}
    },
    Opts4 = case maps:get(user, ConnOpts, <<>>) of
       <<>> ->
           Opts3;
       User ->
           Pass = maps:get(password, ConnOpts, <<>>),
           Opts3#{auth => {basic, User, Pass}}
    end,
    Proto = case Transp of
        http -> tcp;
        https -> tls
    end,
    Port2 = case Port of
        0 when Proto == tcp -> 80;
        0 when Proto == tls -> 443;
        _ -> Port
    end,
    get_conns(Rest, [{{Proto, Ip, Port2}, Opts4}|Acc]).


%% @private
test_connect(_Id, [], _State) ->
    {error, no_connections};

test_connect(Id, [{Conn, Opts}|Rest], State) ->
    case nkhttpc_single:start(Id, [Conn], Opts) of
        {ok, Pid} ->
            nkhttpc_single:stop(Pid),
            ok;
        {error, Error} ->
            case Rest of
                [] ->
                    {error, Error};
                _ ->
                    test_connect(Id, Rest, State)
            end
    end.


%% @private
do_req(Worker, Method, Path, Body, Timeout, Debug) ->
    {Headers, Body2} = case is_map(Body) of
        true ->
            {
                [{<<"Content-Type">>, <<"application/json">>}],
                nklib_json:encode(Body)
            };
        false ->
            {[], Body}
    end,
    case Body2 of
        error ->
            {error, {json_error, Body}};
        _ ->
            case nkhttpc_single:req(Worker, Method, Path, Headers, Body2, Timeout) of
                {ok, Code, RespHds, RespBody, Time} ->
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
                    req_debug(Method, Path, Body, Code, RespBody, Time, Debug),
                    case (Code>=200 andalso Code<300) of
                        true ->
                            {ok, RespBody2, Time};
                        false ->
                            req_error(Code, RespBody2, Debug)
                    end;
                {error, Error} ->
                    {error, Error}
            end
    end.


%%%% @private
%%do_req2(Worker, Method, Path, Headers, Body, Timeout, Debug) ->
%%    case nkhttpc_single:req(Worker, Method, Path, Headers, Body, Timeout) of
%%        {ok, Code, RespHds, RespBody, Time} ->
%%            RespBody2 = case nklib_util:get_value(<<"Content-Type">>, RespHds) of
%%                <<"application/json", _/binary>> ->
%%                    nklib_json:decode(RespBody);
%%                _ ->
%%                    case nklib_util:get_value(<<"content-type">>, RespHds) of
%%                        <<"application/json", _/binary>> ->
%%                            nklib_json:decode(RespBody);
%%                        _ ->
%%                            RespBody
%%                    end
%%            end,
%%            req_debug(Method, Path, Body, Code, RespBody2, Time, Debug),
%%            case (Code >= 200 andalso Code < 300) of
%%                true ->
%%                    {ok, RespBody2, Time};
%%                false ->
%%                    req_error(Code, RespBody2, Debug)
%%            end;
%%        {error, Error} ->
%%            {error, Error}
%%    end.


%% @private
req_error(_Code, #{<<"error">>:=Error}, Debug) ->
    #{<<"type">>:=Type, <<"reason">>:=Reason} = Error,
    case Debug of
        {true, _} ->
            ?REQ_DBG("error ~s (~s): ~p", [Type, Reason, Error]);
        _ ->
            ok
    end,
    {error, get_error(Type, Reason)};

req_error(404, _Body, _Debug) ->
    {error, object_not_found};

req_error(400, Body, _Debug) ->
    lager:notice("NkELASTIC invalid request: ~p", [Body]),
    {error, invalid_request};

req_error(Code, Body, _Debug) ->
    lager:notice("NkELASTIC unrecognized error: ~p ~p", [Code, Body]),
    {error, unknown_error}.


%% @private
get_error(Type, Reason) ->
    case Type of
        <<"index_not_found_exception">> -> index_not_found;
        <<"search_phase_execution_exception">> -> search_error;
        <<"illegal_argument_exception">> -> illegal_argument;
        <<"index_already_exists_exception">> -> index_already_exists;
        _ ->
            lager:notice("NkELASTIC unrecognized error: ~s, ~s", [Type, Reason]),
            Type
    end.


%% @private
req_debug(_Method, _Path, _Body, _Code, _RespBody, _Time, false) ->
    ok;

req_debug(_Method, <<"_cluster/health">>, _Body, _Code, _RespBody, _Time, _Debug) ->
    ok;

req_debug(Method, Path, Body, Code, RespBody, Time, {true, true}) ->
    DbgSendBody = case is_map(Body) orelse is_list(Body) of
        true ->
            <<"-> ", (nklib_json:encode_pretty(Body))/binary, "\n">>;
        false ->
            <<>>
    end,
    DbgRespBody = case is_map(RespBody) orelse is_list(RespBody) of
        true ->
            <<"<- ", (nklib_json:encode_pretty(RespBody))/binary, "\n">>;
        false ->
            <<"<- ", RespBody/binary, "\n">>
    end,
    ?REQ_DBG("~s ~s: ~p ~p msecs\n~s~s",
        [Method, Path, Code, Time, DbgSendBody, DbgRespBody]);

req_debug(Method, Path, _Body, Code, _RespBody, Time, {true, false}) ->
    ?REQ_DBG("~s ~s: ~p ~p msecs", [Method, Path, Code, Time]).


%% @private
to_bin(T) when is_binary(T) -> T;
to_bin(T) -> nklib_util:to_binary(T).
