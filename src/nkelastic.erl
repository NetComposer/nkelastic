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

-module(nkelastic).
-export([request/4, request/5, request/6]).
-export([luerl_request/3]).

%-include_lib("nkpacket/include/nkpacket.hrl").

-define(LLOG(Type, Txt, Args), lager:Type("NkELASTIC "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================

-type method() :: get | post | delete.
-type path() :: string() | binary().
-type body() :: iodata().


%% ===================================================================
%% Public
%% ===================================================================


%% @doc
request(SrvId, PackageId, Method, Path) ->
    request(SrvId, PackageId, Method, Path, <<>>).


%% @doc
request(SrvId, PackageId, Method, Path, Body) ->
    request(SrvId, PackageId, Method, Path, Body, 30000).


%% @doc
-spec request(nkservice:id(), nkpacket:id(), method(), path(), body(), integer()) ->
    {ok, map()|binary(), integer()} |
    {error, {elastic_error, term()}|term()}.

request(SrvId, PackageId, Method, Path, Body, Timeout) ->
    Opts = #{timeout=>Timeout},
    Start = nklib_util:m_timestamp(),
    PackageId2 = to_bin(PackageId),
    case nkpacket_httpc_pool:request({SrvId, PackageId2}, Method, Path, Body, Opts) of
        {ok, Status, RepHds, RepBody} ->
            Time = nklib_util:m_timestamp() - Start,
            Debug = nkservice_util:get_debug(SrvId, {nkelastic, PackageId2, debug}),
            RepBody2 = case nklib_util:get_value(<<"Content-Type">>, RepHds) of
                <<"application/json", _/binary>> ->
                    nklib_json:decode(RepBody);
                _ ->
                    case nklib_util:get_value(<<"content-type">>, RepHds) of
                        <<"application/json", _/binary>> ->
                            nklib_json:decode(RepBody);
                        _ ->
                            RepBody
                    end
            end,
            req_debug(Debug, Method, Path, Body, Status, RepBody, Time),
            case (Status>=200 andalso Status<300) of
                true ->
                    {ok, RepBody2, #{time=>Time}};
                false ->
                    {error, {elastic_error, req_error(Status, RepBody2)}}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% Luerl API
%% ===================================================================


%% @doc
luerl_request(SrvId, PackageId, [Method, Path]) ->
    luerl_request(SrvId, PackageId, [Method, Path, <<>>, 5000]);

luerl_request(SrvId, PackageId, [Method, Path, Body]) ->
    luerl_request(SrvId, PackageId, [Method, Path, Body, 5000]);

luerl_request(SrvId, PackageId, [Method, Path, Body, Timeout]) when is_float(Timeout) ->
    luerl_request(SrvId, PackageId, [Method, Path, Body, round(Timeout)]);

luerl_request(SrvId, PackageId, [Method, Path, Body, Timeout])
        when is_integer(Timeout) ->
    case request(SrvId, PackageId, Method, Path, Body, Timeout) of
        {ok, List, Meta} ->
            [List, Meta];
        {error, {elastic_error, {Error, Txt}}} ->
            [nil, elastic_error, Error, Txt];
        {error, Error} ->
            {Code, Txt} = nkservice_error:error(SrvId, Error),
            [nil, Code, Txt]
    end;

luerl_request(_SrvId, _PackageId, O) ->
    lager:error("NKLOG O ~p", [O]),
    [nil, invalid_parameters].



%% ===================================================================
%% Internal
%% ===================================================================



%% @private
req_error(_Code, #{<<"error">>:=Error}) ->
    #{<<"type">>:=Type, <<"reason">>:=Reason} = Error,
    get_error(Type, Reason);

req_error(404, _Body) ->
    object_not_found;

req_error(400, Body) ->
    ?LLOG(notice, "invalid request: ~p", [Body]),
    invalid_request;

req_error(Code, Body) ->
    ?LLOG(notice, "unrecognized error: ~p ~p", [Code, Body]),
    unknown_error.


%% @private
get_error(Type, Reason) ->
    case Type of
        <<"index_not_found_exception">> -> {index_not_found, Reason};
        <<"search_phase_execution_exception">> -> {search_error, Reason};
        <<"illegal_argument_exception">> -> {illegal_argument, Reason};
        <<"index_already_exists_exception">> -> {index_already_exists, Reason};
        <<"strict_dynamic_mapping_exception">> -> {strict_mapping, Reason};
        <<"index_template_missing_exception">> -> {template_not_found, Reason};
        _ ->
            ?LLOG(notice, "unrecognized error: ~s, ~s", [Type, Reason]),
            {Type, Reason}
    end.


%% @private
req_debug(_Debug, _Method, <<"_cluster/health">>, _Body, _Code, _RespBody, _Time) ->
    ok;

req_debug(false, _Method, _Path, _Body, _Code, _RespBody, _Time) ->
    ok;

req_debug(true, Method, Path, _Body, Code, _RespBody, Time) ->
    ?LLOG(debug, "~s ~s: ~p ~p msecs", [Method, Path, Code, Time]);

req_debug(full, Method, Path, Body, Code, RespBody, Time) ->
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
    ?LLOG(debug, "~s ~s: ~p ~p msecs\n~s~s",
        [Method, Path, Code, Time, DbgSendBody, DbgRespBody]).



%% @private
to_bin(T) when is_binary(T) -> T;
to_bin(T) -> nklib_util:to_binary(T).
