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

%% @doc NkELASTIC external API

-module(nkelastic_api).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([health/1, get_nodes/1]).
-export([list_indices/1, get_indices/1, get_index/2]).
-export([get_count/1, get_count/2]).
-export([create_index/3, delete_index/2, update_index/3, update_or_create_index/3]).
-export([get_template/2, create_template/3, delete_template/2, get_all_templates/1]).
-export([update_analysis/3, add_mapping/4]).
-export([get_aliases/1, get_aliases/2, add_alias/4, delete_alias/3]).
-export([get/4, put/5, put_and_wait/5, delete/4, delete_by_query/4, delete_all/3]).
-export([search/5, count/5, explain/5, iterate_start/5, iterate_next/2, iterate_fun/7]).

-type id() :: nkservice:id().
-type index() :: binary() | string().
-type obj_id() :: binary() | string().
-type type() :: binary() | string().
-type error() :: {es_error, binary(), binary()} | term().
-type status() :: geen | yellow | red.
-type query() :: nkelastic_search:query().
-type search_opts() :: nkelastic_search:search_opts().




% https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html

%% ===================================================================
%% Public
%% ===================================================================

%% @doc Gets cluster health
-spec health(id) ->
	{ok, status(), map()} | {error, error()}.

health(Id) ->
    case request(Id, get, "_cluster/health") of
    	{ok, #{<<"status">>:=Status}=Data} ->
    		{ok, binary_to_atom(Status, latin1), Data};
    	{error, Error} ->
    		{error, Error}
    end.


%% @doc Gets node info
%% https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-nodes.html
-spec get_nodes(id()) ->
	{ok, [map()]} | {error, error()}.

get_nodes(Id) ->  
    request(Id, get, "_cat/nodes?format=json").


%% @doc Lists all indices
-spec list_indices(id()) ->
	{ok, [binary()]} | {error, error()}.

list_indices(Id) ->
    request_lines(Id, get, "_cat/indices?h=index").


%% @doc Gets all indices with detailed info
-spec get_indices(id()) ->
	{ok, map()} | {error, error()}.

get_indices(Id) ->
    request(Id, get, "_all").


%% @doc Gets info about and index
-spec get_index(id(), index()) ->
	{ok, map()} | {error, error()}.

get_index(Id, Index) ->
    request(Id, get, Index).


%% @doc Get number of objects
-spec get_count(id()) ->
	{ok, integer()} | {error, error()}.

get_count(Id) ->
    get_count(Id, all).


%% @doc Get number of objects for an index
-spec get_count(id(), index()) ->
	{ok, integer()} | {error, error()}.

get_count(Id, Index) ->
	Msg = ["_cat/count", post_index(Index), "?h=count"],
    case request_lines(Id, get, Msg) of
        {ok, [Count]} ->
            {ok, to_int(Count)};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Creates an index
%% Name: letters, numbers, ".,-&_"
%% Options: number_of_shards, number_of_replicas, index_refresh_interval
%% Also aliases (#{aliases => #{alias1 => #{}})
%% and mappings (#{mappings => #{type1 => #{properties => ...}}})
-spec create_index(id(), index(), map()) ->
	ok | {error, error()}.

create_index(Id, Index, Opts) ->
    Body = index_params(Opts),
    request(Id, put, Index, Body).


%% @doc Deletes an index
-spec delete_index(id(), index()) ->
	ok | {error, error()}.

delete_index(Id, Index) ->
    request(Id, delete, Index).


%% @doc Updates an index
%% Options: refresh_interval, number_of_replicas
-spec update_index(id(), index(), map()) ->
	ok | {error, error()}.

update_index(Id, Index, Opts) ->
    Body = #{index => Opts},
    request(Id, put, [Index, "/_settings"], Body).


%% @doc Updates analysis
-spec update_analysis(id(), index(), map()) ->
	ok | {error, error()}.

update_analysis(Id, Index, Opts) ->
    Body = #{analysis => Opts},
    request(Id, put, [Index, "/_settings"], Body).


%% @doc Tries to update an index, or create it
%% Options
update_or_create_index(Id, Index, Opts) ->
    case update_index(Id, Index, Opts) of
        {error, index_not_found} ->
            lager:notice("NkELASTIC: Index ~s not found, creating it", [Index]),
            create_index(Id, Index, Opts);
        Other ->
            Other
    end.


%% @doc Creates an index template
%% Same parameters as for indices
-spec create_template(id(), Name::binary(), map()) ->
    ok | {error, error()}.

create_template(Id, Name, #{template:=Template}=Opts) ->
    Body1 = index_params(maps:remove(template, Opts)),
    Body2 = Body1#{template=>to_bin(Template)},
    request(Id, put, ["_template/", Name], Body2).


%% @doc Deletes an template
-spec delete_template(id(), Name::binary()) ->
    ok | {error, error()}.

delete_template(Id, Name) ->
    request(Id, delete, ["_template/", Name]).


%% @doc Gets a template
-spec get_template(id(), Name::binary()) ->
    {ok, map()} | {error, error()}.

get_template(Id, Name) ->
    request(Id, get, ["_template/", Name]).


%% @doc Gets all templates
-spec get_all_templates(id()) ->
    {ok, [binary()]} | {error, error()}.

get_all_templates(Id) ->
    case request(Id, get, "_template") of
        {ok, Map} ->
            {ok, maps:keys(Map)};
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
-spec add_mapping(id(), index(), type(), map()) ->
	ok | {error, term()}.

add_mapping(Id, Index, Type, Mappings) ->
    {Metas, Props} = extract_mappings(Mappings),
    Body = Metas#{properties=>Props},
    request(Id, put, [Index, "/", to_bin(Type), "/_mapping"], Body).


%% @doc Get all indices and their aliases
-spec get_aliases(id()) ->
	{ok, #{index() => [Alias::binary()]}} | {error, error()}.

get_aliases(Id) ->
    case request(Id, get, ["_aliases"]) of
    	{ok, Map} ->
    		List = lists:foldl(
    			fun({Index, #{<<"aliases">>:=Aliases}}, Acc) ->
    				[{Index, maps:keys(Aliases)}|Acc]
    			end,
    			[],
    			maps:to_list(Map)),
    		{ok, maps:from_list(List)};
    	{error, Error} ->
    		{error, Error}
    end.


%% @doc Get aliases for an index
-spec get_aliases(id(), index()) ->
	{ok, #{index() => [Alias::binary()]}}.

get_aliases(Id, Index) ->
    case request(Id, get, [Index, "/_aliases"]) of
    	{ok, Map} ->
    		[{_, #{<<"aliases">>:=Aliases}}] = maps:to_list(Map),
    		{ok, maps:keys(Aliases)};
    	{error, Error} ->
    		{error, Error}
    end.


%% @doc Adds an alias to an index
%% Opts can include
% #{filter => #{key=>val}, index_routing=>... search_routing=>...}
-spec add_alias(id(), index(), binary(), map()) ->
	ok | {error, term()}.

add_alias(Id, Index, Name, Opts) ->
    request(Id, put, [Index, "/_aliases/", to_bin(Name)], Opts).


%% @doc Removes an alias from an index
-spec delete_alias(id(), index(), binary()) ->
	ok | {error, term()}.

delete_alias(Id, Index, Name) ->
    request(Id, delete, [Index, "/_alias/", to_bin(Name)]).



%% @doc Gets an object by id
-spec get(id(), index(), type(), obj_id()) ->
	{ok, map(), integer()} | {error, term()}.

get(Id, Index, Type, ObjId) ->
    case request(Id, get, [Index, "/", to_bin(Type), "/", ObjId]) of
    	{ok, #{
    		<<"_source">> := Src,
    		<<"_version">> := Vsn
    	}} ->
    		{ok, Src, Vsn};
    	{error, Error} ->
    		{error, Error}
    end.


%% @doc Puts an object by id
-spec put(id(), index(), type(), obj_id(), map()) ->
	{ok, integer()} | {error, term()}.

put(Id, Index, Type, ObjId, Obj) ->
    case request_data(Id, put, [Index, "/", to_bin(Type), "/", ObjId], Obj) of
    	{ok, #{<<"_version">>:=Vsn}} -> {ok, Vsn};
    	{error, Error} -> {error, Error}
    end.


%% @doc Puts an object by id
-spec put_and_wait(id(), index(), type(), obj_id(), map()) ->
    {ok, integer()} | {error, term()}.

put_and_wait(Id, Index, Type, ObjId, Obj) ->
    case request_data(Id, put, [Index, "/", to_bin(Type), "/", ObjId, "?refresh=true"], Obj) of
        {ok, #{<<"_version">>:=Vsn}} -> {ok, Vsn};
        {error, Error} -> {error, Error}
    end.


%% @doc Deletes an object by id
-spec delete(id(), index(), type(), obj_id()) ->
	ok | {error, term()}.

delete(Id, Index, Type, ObjId) ->
    request(Id, delete, [Index, "/", to_bin(Type), "/", ObjId]).


%% @doc Deletes all objects from a query
-spec delete_by_query(id(), index(), type(), map()) ->
    {ok, map()} | {error, term()}.

delete_by_query(Id, Index, Type, Query) ->
    Url =  index_url(delete_by_query, Index, Type, <<>>),
    request(Id, post, Url, #{query=>Query}).


%% @doc Gets all objects having a type
-spec delete_all(id(), index(), type()) ->
    {ok, map()} | {error, term()}.

delete_all(Id, Index, Type) ->
    delete_by_query(Id, Index, Type, #{match_all=>#{}}).


%% @doc Search
-spec search(id(), index(), type(), query(), search_opts()) ->
    {ok, integer(), Obj::[map()], Aggs::map(), Meta::map()} | {error, term()}.

search(Id, Index, Type, Query, Opts) ->
    case nkelastic_search:parse(Query, Opts) of
        {ok, Body} ->
            Url =  index_url(search, Index, Type, <<>>),
            case request(Id, post, Url, Body) of
                {ok, Reply} ->
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
                    Meta = #{time=>Time, timeout=>TimedOut},
                    {ok, Total, Hits, Aggs, Meta};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Count
-spec count(id(), index(), type(), query(), search_opts()) ->
    {ok, integer()} | {error, term()}.

count(Id, Index, Type, Query, Opts) ->
    case nkelastic_search:parse(Query, Opts) of
        {ok, Body} ->
            Url = index_url(count, Index, Type, <<>>),
            case request(Id, post, Url, Body) of
                {ok, #{<<"count">>:=Count}} ->
                    {ok, Count};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Search explain
-spec explain(id(), index(), type(), query(), search_opts()) ->
    {ok, integer()} | {error, term()}.

explain(Id, Index, Type, Query, Opts) ->
    case nkelastic_search:parse(Query, Opts) of
        {ok, Body} ->
            Url = index_url(explain, Index, Type, <<>>),
            case request(Id, post, Url, Body) of
                {ok, Data} ->
                    {ok, Data};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Iterate
-spec iterate_start(id(), index(), type(), query(), search_opts()) ->
    {ok, binary(), integer(), Obj::[map()], Meta::map()} | {error, term()}.

iterate_start(Id, Index, Type, Query, Opts) ->
    case nkelastic_search:parse(Query, Opts) of
        {ok, Body} ->
            Url =  index_url(search, Index, Type, <<"?scroll=1m">>),
            case request(Id, post, Url, Body) of
                {ok, Reply} ->
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
                    Meta = #{time=>Time, timeout=>TimedOut},
                    {ok, ScrollId, Total, Hits, Meta};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.

%% @doc Search
-spec iterate_next(id(), binary()) ->
    {ok, binary(), integer(), Obj::[map()], Meta::map()} | {error, term()}.

iterate_next(Id, ScrollId) ->
    Body = #{scroll => <<"1m">>, scroll_id=>ScrollId},
    case request(Id, post, <<"_search/scroll">>, Body) of
        {ok, Reply} ->
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
            Meta = #{time=>Time, timeout=>TimedOut},
            {ok, ScrollId2, Total, Hits, Meta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Iterate
-spec iterate_fun(id(), index(), type(), query(), search_opts(),
                  fun((map(), term()) -> term()), term()) ->
    {ok, binary(), integer(), Obj::[map()], Meta::map()} | {error, term()}.

iterate_fun(Id, Index, Type, Query, Opts, Fun, Acc0) ->
    case iterate_start(Id, Index, Type, Query, Opts) of
        {ok, _ScrollId, _N, [], _} ->
            {ok, Acc0};
        {ok, ScrollId, _N, Objs, _} ->
            Acc2 = lists:foldl(
                fun(Obj, Acc) -> iterate_fun_process(Obj, Fun, Acc) end,
                Acc0,
                Objs
            ),
            iterate_fun2(Id, ScrollId, Fun, Acc2);
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
iterate_fun2(Id, ScrollId, Fun, Acc0) ->
    case iterate_next(Id, ScrollId) of
        {ok, _ScrollId2, _N, [], _} ->
            {ok, Acc0};
        {ok, ScrollId2, _N, Objs, _} ->
            Acc2 = lists:foldl(
                fun(Obj, Acc) -> iterate_fun_process(Obj, Fun, Acc) end,
                Acc0,
                Objs
            ),
            iterate_fun2(Id, ScrollId2, Fun, Acc2);
        {error, Error} ->
            {error, Error}
    end.


%% @private
iterate_fun_process(#{<<"_id">>:=ObjId}=Data, Fun, Acc) ->
    Base = maps:get(<<"_source">>, Data, #{}),
    Obj = Base#{<<"obj_id">>=>ObjId},
    Fun(Obj, Acc).


%% @private
request(Id, Method, Path) ->
    request(Id, Method, Path, <<>>).


%% @private
request(Id, Method, Path, Body) ->
    case request_data(Id, Method, Path, Body) of
    	{ok, _} when Method==put; Method==delete -> ok;
    	{ok, Data} -> {ok, Data};
    	{error, Error} -> {error, Error}
   	end.


%% @private
request_data(Id, Method, Path, Body) ->
    nkelastic_srv:request(Id, Method, Path, Body).


%% @private
request_lines(Id, Method, Path) ->
    case request_data(Id, Method, Path, <<>>) of
        {ok, List} ->
            List2 = binary:split(List, <<"\n">>, [global]),
            [<<>> | List3] = lists:reverse(List2),
            {ok, lists:reverse(List3)};
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
