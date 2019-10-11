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

-export([health/1, get_nodes/1, get_count/1]).
-export([list_indices/1, get_indices/1, get_index/1]).
-export([create_index/2, delete_index/1, update_index/2, update_or_create_index/2]).
-export([update_analysis/2, add_mapping/2]).
-export([get_template/2, create_template/3, delete_template/2, get_all_templates/1]).
-export([get_aliases/1, add_alias/3, delete_alias/2]).
-export([get/2, put/3, delete/2, delete_by_query/2, delete_all/1]).
-export([search/2, count/2, explain/2, iterate_start/2, iterate_next/2, iterate_fun/4]).

%% ===================================================================
%% Types
%% ===================================================================


-type srv_id() :: nkservice:id().
-type cluster_id() :: binary().
-type index() :: binary() | string().
-type name() :: binary() | string().
-type obj_id() :: binary() | string().
-type obj() :: map().
-type type() :: binary() | string().
-type error() :: {es_error, binary(), binary()} | term().
-type status() :: geen | yellow | red.
-type query() :: nkelastic_search:query().


-type opts() ::
    #{
        srv_id => srv_id(),
        cluster_id => cluster_id(),
        index => index(),
        type => type(),
        refresh => boolean()
    }.

-type index_opts() ::
    #{
        mappings => mappings(),
        aliases => aliases(),
        number_of_shards => integer(),
        number_of_replicas => integer(),
        refresh_interval => binary(),      %% "-1", "5s"
        'mapper.dynamic' => boolean(),
        atom() => term()
    }.

-type aliases() ::
    #{
        name() => map()
    }.

-type mappings() ::
    #{
        type() => #{properties => map()}
    }.


-type analysis() :: #{}.

-type iterate_fun() ::
    fun((map(), term()) -> {ok, term()} | {error, term()}).

-type resp_meta() ::
    #{
        time => integer(),
        vsn => integer(),
        es_time => integer()
    }.


%% ===================================================================
%% Cluster commands
%% ===================================================================


%% @doc Gets cluster health
-spec health(opts()) ->
    {ok, status(), map(), resp_meta()} | {error, error()}.

health(Opts) ->
    case request(get, "_cluster/health", Opts) of
        {ok, #{<<"status">>:=Status}=Data, Meta} ->
            {ok, binary_to_atom(Status, latin1), Data, Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets node info
%% https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-nodes.html
-spec get_nodes(opts()) ->
    {ok, [map()], resp_meta()} | {error, error()}.

get_nodes(Opts) ->
    request(get, "_cat/nodes?format=json", Opts).


%% @doc Get number of objects for an index
-spec get_count(opts()) ->
    {ok, integer(), resp_meta()} | {error, error()}.

get_count(#{index:=Index}=Opts) ->
    Msg = ["_cat/count", post_index(Index), "?h=count"],
    case request_lines(get, Msg, Opts) of
        {ok, [Count], Meta} ->
            {ok, to_int(Count), Meta};
        {error, Error} ->
            {error, Error}
    end;

get_count(Opts) ->
    get_count(Opts#{index=>all}).



%% ===================================================================
%% Indices commands
%% ===================================================================


%% @doc Lists all indices
-spec list_indices(opts()) ->
    {ok, [binary()], resp_meta()} | {error, error()}.

list_indices(Opts) ->
    request_lines(get, "_cat/indices?h=index", Opts).


%% @doc Gets all indices with detailed info
-spec get_indices(opts()) ->
    {ok, map(), resp_meta()} | {error, error()}.

get_indices(Opts) ->
    request(get, "_all", Opts).


%% @doc Gets info about and index
-spec get_index(opts()) ->
    {ok, map(), resp_meta()} | {error, error()}.

get_index(#{index:=Index}=Opts) ->
    request(get, Index, Opts).


%% @doc Creates an index
-spec create_index(index_opts(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

create_index(IndexOpts, #{index:=Index}=Opts) ->
    Body = index_params(IndexOpts),
    request(put, Index, Body, Opts).


%% @doc Deletes an index
-spec delete_index(opts()) ->
    {ok, resp_meta()} | {error, error()}.

delete_index(#{index:=Index}=Opts) ->
    request(delete, Index, Opts).


%% @doc Updates an index
%% Options: refresh_interval, number_of_replicas
-spec update_index(index_opts(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

update_index(IndexOpts, #{index:=Index}=Opts) ->

    request(put, [Index, "/_settings"], #{index=>IndexOpts}, Opts).


%% @doc Updates analysis
-spec update_analysis(analysis(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

update_analysis(Analysis, #{index:=Index}=Opts) ->
    request(put, [Index, "/_settings"], #{analysis=>Analysis}, Opts).


%% @doc Tries to update an index, or create it
update_or_create_index(IndexOpts, #{index:=Index}=Opts) ->
    case update_index(IndexOpts, Opts) of
        {error, {index_not_found, _}} ->
            lager:notice("NkELASTIC: Index ~s not found, creating it", [Index]),
            create_index(IndexOpts, Opts);
        {ok, Meta} ->
            {ok, Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Set a mapping
%% https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
%% Sample:
%% #{
%%    '_all' => #{disabled=>true},
%%    id => #{type=>string, store=>yes, index=>not_analyzed}
%%}
%% Metafields: https://www.elastic.co/guide/en/elasticsearch/
%%                     reference/current/mappnkseing-fields.html
%% Type is dynamic (see nkelastic_plugin)
%% - true (default)
%% - false: Not indexed, but added to source
%% - strict: launch exception
%%
%% https://stackoverflow.com/questions/33263673/disable-dynamic-mapping-creation-for-only-specific-indexes-on-elasticsearch

-spec add_mapping(map(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

add_mapping(Mapping, #{index:=Index, type:=Type}=Opts) ->
    {Metas, Props} = extract_mappings(Mapping),
    Dynamic = maps:get(type_is_dynamic, Opts, false),
    Body = Metas#{dynamic=>Dynamic, properties=>Props},
    request(put, [Index, "/", to_bin(Type), "/_mapping"], Body, Opts).




%% ===================================================================
%% Template commands
%% ===================================================================

%% @doc Gets a template
-spec get_template(name(), opts()) ->
    {ok, map(), resp_meta()} | {error, error()}.

get_template(Name, Opts) ->
    request(get, ["_template/", Name], Opts).


%% @doc Gets all templates
-spec get_all_templates(opts()) ->
    {ok, [binary()], resp_meta()} | {error, error()}.

get_all_templates(Opts) ->
    case request(get, "_template", Opts) of
        {ok, Map, Meta} ->
            {ok, maps:keys(Map), Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Creates an index template
%% Same parameters as for indices
-spec create_template(name(), index_opts(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

create_template(Name, IndexOpts, Opts) ->
    Body1 = index_params(IndexOpts),
    Body2 = Body1#{template=><<(to_bin(Name))/binary, $*>>},
    request(put, ["_template/", Name], Body2, Opts).


%% @doc Deletes an template
-spec delete_template(name(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

delete_template(Name, Opts) ->
    request(delete, ["_template/", Name], Opts).


%% ===================================================================
%% Aliases
%% ===================================================================


%% @doc Get all indices and their aliases
-spec get_aliases(opts()) ->
    {ok, #{index() => [Alias::binary()]}, resp_meta()} | {error, error()}.

get_aliases(#{index:=Index}=Opts) ->
    case request(get, [Index, "/_aliases"], Opts) of
        {ok, Map, Meta} ->
            [{_, #{<<"aliases">>:=Aliases}}] = maps:to_list(Map),
            {ok, #{Index=>maps:keys(Aliases)}, Meta};
        {error, Error} ->
            {error, Error}
    end;

get_aliases(Opts) ->
    case request(get, ["_aliases"], Opts) of
        {ok, Map, Meta} ->
            List = lists:foldl(
                fun({Index, #{<<"aliases">>:=Aliases}}, Acc) ->
                    [{Index, maps:keys(Aliases)}|Acc]
                end,
                [],
                maps:to_list(Map)),
            {ok, maps:from_list(List), Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Adds an alias to an index
%% Opts can include
% #{filter => #{key=>val}, index_routing=>... search_routing=>...}
-spec add_alias(name(), map(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

add_alias(Name, AliasOpts, #{index:=Index}=Opts) ->
    request(put, [Index, "/_aliases/", to_bin(Name)], AliasOpts, Opts).


%% @doc Removes an alias from an index
-spec delete_alias(name(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

delete_alias(Name, #{index:=Index}=Opts) ->
    request(delete, [Index, "/_alias/", to_bin(Name)], Opts).



%% ===================================================================
%% Objects
%% ===================================================================


%% @doc Gets an object by id
-spec get(obj_id(), opts()) ->
    {ok, map(), resp_meta()} | {error, term()}.

get(ObjId, #{index:=Index, type:=Type}=Opts) ->
    case request(get, [Index, "/", to_bin(Type), "/", ObjId], Opts) of
        {ok, Data, Meta} ->
            #{
                <<"_source">> := Src,
                <<"_version">> := Vsn
            } = Data,
            {ok, Src, Meta#{vsn=>Vsn}};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Puts an object by id
-spec put(obj_id(), obj(), opts()) ->
    {ok, resp_meta()} | {error, term()}.

put(ObjId, Obj, #{index:=Index, type:=Type}=Opts) ->
    Refresh = case Opts of
        #{refresh:=true} -> "?refresh=true";
        _ -> ""
    end,
    case request_data(put, [Index, "/", to_bin(Type), "/", ObjId, Refresh], Obj, Opts) of
        {ok, Data, Meta} ->
            #{
                <<"_version">> := Vsn
            } = Data,
            Created = case Data of
                #{<<"created">> := IsCreated} ->
                    IsCreated;
                #{<<"result">> := <<"created">>} ->
                    true;
                _ ->
                    false
            end,
            {ok, Meta#{vsn=>Vsn, created=>Created}};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Deletes an object by id
-spec delete(obj_id(), opts()) ->
    {ok, resp_meta()} | {error, error()}.

delete(ObjId, #{index:=Index, type:=Type}=Opts) ->
    Refresh = case Opts of
        #{refresh:=true} -> "?refresh=true";
        _ -> ""
    end,
    request(delete, [Index, "/", to_bin(Type), "/", ObjId, Refresh], Opts).


%% @doc Deletes all objects from a query
-spec delete_by_query(query(), opts()) ->
    {ok, map(), resp_meta()} | {error, term()}.

delete_by_query(Query, #{index:=Index, type:=Type}=Opts) ->
    Url =  index_url(delete_by_query, Index, Type, <<>>),
    request(post, Url, Query, Opts).


%% @doc Gets all objects having a type
-spec delete_all(opts()) ->
    {ok, map(), resp_meta()} | {error, term()}.

delete_all(Opts) ->
    delete_by_query(#{match_all=>#{}}, Opts).


%% ===================================================================
%% Search
%% ===================================================================


%% @doc Search
%% Query can be generated with nkelastic_search:query/1
-spec search(query(), opts()) ->
    {ok, integer(), [Obj::map()], Aggs::map(), resp_meta()} | {error, term()}.

search(Query, #{index:=Index, type:=Type}=Opts) ->
    Url =  index_url(search, Index, Type, <<>>),
    % lager:info("ES Query: ~s", [nklib_json:encode_pretty(Query)]),
    case request(post, Url, Query, Opts) of
        {ok, Reply, Meta} ->
            #{
                <<"took">> := Time,
                <<"timed_out">> := TimedOut,
                <<"hits">>:= #{
                    <<"total">>:=Total, <<"hits">>:=Hits
                }
            } = Reply,
            %% lager:info("Query took ~p msecs", [Time]),
            %% lager:info("~s", [nklib_json:encode_pretty(Reply)]),
            Aggs = maps:get(<<"aggregations">>, Reply, #{}),
            Meta2 = Meta#{es_time=>Time, timeout=>TimedOut},
            {ok, Total, Hits, Aggs, Meta2};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Count
-spec count(query(), opts()) ->
    {ok, integer(), resp_meta()} | {error, term()}.

count(Query, #{index:=Index, type:=Type}=Opts) ->
    Url = index_url(count, Index, Type, <<>>),
    case request(post, Url, Query, Opts) of
        {ok, #{<<"count">>:=Count}, _Meta} ->
            {ok, Count};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Search explain
-spec explain(query(), opts()) ->
    {ok, integer(), resp_meta()} | {error, term()}.

explain(Query, #{index:=Index, type:=Type}=Opts) ->
    Url = index_url(explain, Index, Type, <<>>),
    case request(post, Url, Query, Opts) of
        {ok, Data} ->
            {ok, Data};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Iterate
-spec iterate_start(query(), opts()) ->
    {ok, binary(), integer(), Obj::[map()], Meta::map(), resp_meta()} | {error, term()}.

iterate_start(Query, #{index:=Index, type:=Type}=Opts) ->
    Url =  index_url(search, Index, Type, <<"?scroll=1m">>),
    case request(post, Url, Query, Opts) of
        {ok, Reply, Meta} ->
            #{
                <<"_scroll_id">> := ScrollId,
                <<"took">> := Time,
                <<"timed_out">> := TimedOut,
                <<"hits">>:= #{
                    <<"total">>:=Total, <<"hits">>:=Hits
                }
            } = Reply,
            %% lager:info("Query took ~p msecs", [Time]),
            %% lager:info("~s", [nklib_json:encode_pretty(Reply)]),
            Meta2 = Meta#{time=>Time, timeout=>TimedOut},
            {ok, ScrollId, Total, Hits, Meta2};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Search
-spec iterate_next(term(), opts()) ->
    {ok, binary(), integer(), Obj::[map()], Meta::map(), resp_meta()} | {error, term()}.

iterate_next(ScrollId, Opts) ->
    Body = #{scroll => <<"1m">>, scroll_id=>ScrollId},
    case request(post, <<"_search/scroll">>, Body, Opts) of
        {ok, Reply, Meta} ->
            #{
                <<"_scroll_id">> := ScrollId2,
                <<"took">> := Time,
                <<"timed_out">> := TimedOut,
                <<"hits">>:= #{
                    <<"total">>:=Total, <<"hits">>:=Hits
                }
            } = Reply,
            %% lager:info("Query took ~p msecs", [Time]),
            %% lager:info("~s", [nklib_json:encode_pretty(Reply)]),
            Meta2 = Meta#{time=>Time, timeout=>TimedOut},
            {ok, ScrollId2, Total, Hits, Meta2};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Iterate
-spec iterate_fun(query(), iterate_fun(), Acc::term(), opts()) ->
    {ok, term(), resp_meta()} | {error, term()}.

iterate_fun(Query, Fun, Acc, Opts) ->
    case iterate_start(Query, Opts) of
        {ok, _ScrollId, _N, [], _} ->
            {ok, Acc};
        {ok, ScrollId, _N, Objs, _} ->
            case iterate_fun_fold(Objs, Fun, Acc) of
                {ok, Acc2} ->
                    iterate_fun2(ScrollId, Fun, Acc2, Opts);
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% ===================================================================
%% Internal
%% ===================================================================

index_params(Opts) ->
    List = [
        {settings, #{index => maps:without([mappings, aliases], Opts)}},
        case maps:find(mappings, Opts) of
            {ok, Mappings} ->
                {mappings, Mappings};
            error ->
                []
        end,
        case maps:find(aliases, Opts) of
            {ok, Aliases} ->
                {aliases, Aliases};
            error ->
                []
        end
    ],
    maps:from_list(lists:flatten(List)).



%% @private
extract_mappings(Mappings) ->
    extract_mappings(maps:to_list(Mappings), [], []).


%% @private
extract_mappings([], Metas, Prop) ->
    {maps:from_list(Metas), maps:from_list(Prop)};

extract_mappings([{Key, Val}|Rest], Metas, Prop) ->
    case to_bin(Key) of
        <<"index">>=B ->
            extract_mappings(Rest, [{B, Val}|Metas], Prop);
        <<$_, _/binary>>=B ->
            extract_mappings(Rest, [{B, Val}|Metas], Prop);
        B ->
            extract_mappings(Rest, Metas, [{B, Val}|Prop])
    end.


%% @private
iterate_fun2(ScrollId, Fun, Acc, Opts) ->
    case nkelastic:iterate_next(ScrollId, Opts) of
        {ok, _ScrollId2, _N, [], _} ->
            {ok, Acc};
        {ok, ScrollId2, _N, Objs, _} ->
            case iterate_fun_fold(Objs, Fun, Acc) of
                {ok, Acc2} ->
                    iterate_fun2(ScrollId2, Fun, Acc2, Opts);
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private
iterate_fun_fold([], _Fun, Acc) ->
    {ok, Acc};

iterate_fun_fold([Obj|Rest], Fun, Acc) ->
    case iterate_fun_process(Obj, Fun, Acc) of
        {ok, Acc2} ->
            iterate_fun_fold(Rest, Fun, Acc2);
        {error, Error} ->
            {error, Error}
    end.

%% @private
iterate_fun_process(#{<<"_id">>:=ObjId}=Data, Fun, Acc) ->
    Base = maps:get(<<"_source">>, Data, #{}),
    Obj = Base#{<<"obj_id">>=>ObjId},
    Fun(Obj, Acc).


%% @private
request(Method, Path, Opts) ->
    request(Method, Path, <<>>, Opts).


%% @private
request(Method, Path, Body, Opts) ->
    case request_data(Method, Path, Body, Opts) of
        {ok, _, Meta} when Method==put; Method==delete -> {ok, Meta};
        {ok, Data, Meta} -> {ok, Data, Meta};
        {error, Error} -> {error, Error}
    end.


%% @private
request_data(Method, Path, Body, Opts) ->
    #{srv_id:=SrvId} = Opts,
    ClusterId = maps:get(cluster_id, Opts, <<"main">>),
    Path2 = iolist_to_binary([<<"/">>, Path]),
    case nkelastic_server:req(SrvId, ClusterId, Method, Path2, Body) of
        {ok, Resp, Time} ->
            {ok, Resp, #{time=>Time}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
request_lines(Method, Path, Opts) ->
    case request_data(Method, Path, <<>>, Opts) of
        {ok, List, Meta} ->
            List2 = binary:split(List, <<"\n">>, [global]),
            [<<>> | List3] = lists:reverse(List2),
            {ok, lists:reverse(List3), Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @private
index_url(Op, Index, Type, Str) ->
    case Type of
        <<>> ->    [Index, "/_", to_bin(Op), Str];
        <<"*">> -> [Index, "/_", to_bin(Op), Str];
        _ ->       [Index, "/", to_bin(Type), "/_", to_bin(Op), Str]
    end.


%% @private
post_index(all) -> [];
post_index(Index) -> ["/", Index].


%% @private
to_bin(Term) ->
    nklib_util:to_binary(Term).

%% @private
to_int(Term) ->
    nklib_util:to_integer(Term).
