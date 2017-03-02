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
-export([create_index/3, delete_index/2, update_index/3]).
-export([update_analysis/3, add_mapping/4]).
-export([get_aliases/1, get_aliases/2, add_alias/4, delete_alias/3]).
-export([get/4, put/5, delete/4, delete_all/3]).
-export([url_search/4, list/4]).
-export([spanish_ascii_analyzer/0]).

-type id() :: nkservice:id().
-type index() :: binary() | string().
-type obj_id() :: binary() | string().
-type type() :: binary() | string().
-type error() :: {es_error, binary(), binary()} | term().
-type status() :: geen | yellow | red.
-type query() :: binary() | string().


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
-spec create_index(id(), index(), map()) ->
	ok | {error, error()}.

create_index(Id, Index, Opts) ->
    Body = #{
        settings => #{
            index => Opts
        }
    },
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


%% @doc Updates analisis
-spec update_analysis(id(), index(), map()) ->
	ok | {error, error()}.

update_analysis(Id, Index, Opts) ->
    Body = #{analysis => Opts},
    request(Id, put, [Index, "/_settings"], Body).


%% @doc Set a mapping
%% https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
%% Sample:
%% #{
%%    '_all' => #{disabled=>true},
%%    id => #{type=>string, store=>yes, index=>not_analyzed}
%%}
%% Metafields: https://www.elastic.co/guide/en/elasticsearch/reference/current/mappnkseing-fields.html
-spec add_mapping(id(), index(), type(), map()) ->
	ok | {error, term()}.

add_mapping(Id, Index, Type, Mappings) ->
    {Metas, Props} = extract_mappings(Mappings),
    Body = Metas#{properties=>Props},
    request(Id, put, [Index, "/", Type, "/_mapping"], Body).


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
    request(Id, put, [Index, "/_aliases/", Name], Opts).


%% @doc Removes an alias from an index
-spec delete_alias(id(), index(), binary()) ->
	ok | {error, term()}.

delete_alias(Id, Index, Name) ->
    request(Id, delete, [Index, "/_alias/", Name]).


%% @doc Gets an object by id
-spec get(id(), index(), type(), obj_id()) ->
	{ok, map(), integer()} | {error, term()}.

get(Id, Index, Type, ObjId) ->
    case request(Id, get, [Index, "/", Type, "/", ObjId]) of
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
    case request_data(Id, put, [Index, "/", Type, "/", ObjId], Obj) of
    	{ok, #{<<"_version">>:=Vsn}} -> {ok, Vsn};
    	{error, Error} -> {error, Error}
    end.


%% @doc Gets an object by id
-spec delete(id(), index(), type(), obj_id()) ->
	ok | {error, term()}.

delete(Id, Index, Type, ObjId) ->
    request(Id, delete, [Index, "/", Type, "/", ObjId]).


%% @doc Gets all objects having a type
-spec delete_all(id(), index(), type()) ->
	ok | {error, term()}.

delete_all(Id, Index, Type) ->
    Body = #{query=>#{match_all=>#{}}},
    request(Id, post, [Index, "/", Type, "/_delete_by_query"], Body).


%% @doc Url search
-spec url_search(id(), index(), type(), query()) ->
	{ok, integer(), [map()]} | {error, term()}.

url_search(Id, Index, Type, Str) ->
    case request(Id, get, [Index, "/", Type, "/_search", Str]) of
        {ok, #{<<"hits">>:=#{<<"total">>:=Total, <<"hits">>:=Hits}}} ->
            {ok, Total, Hits};
        {error, Error} ->
            {error, Error}
    end.


-type list_opts() ::
    #{
        fields => [binary()],                          %% Special case for [<<"_all">>]
        filter => #{Key::binary() => Val::binary()},
        size => integer(),
        from => integer(),
        sort_by => [Field::binary()],
        sort_order => asc | desc,
        sort_fields => #{binary() => binary()}   %% Change the sorting field
    }.


%% @doc Powerful listing
-spec list(id(), index(), type(), list_opts()) ->
    {ok, integer(), [map()]} | {error, term()}.

list(Id, Index, Type, Opts) ->
    Source = list_get_source(Opts),
    Query = case Opts of
        #{filter:=Filter} when is_map(Filter) -> 
            list_get_query(maps:to_list(Filter), []);
        _ -> 
            <<>>
    end,
    From = case Opts of
        #{from:=F} -> <<"&from=", (to_bin(F))/binary>>;
        _ -> <<>>
    end,
    Size = case Opts of
        #{size:=S} -> <<"&size=", (to_bin(S))/binary>>;
        _ -> <<>>
    end,
    Sort = list_get_sort(Opts),
    Str = ["?", Source, Query, From, Size, Sort],
    case url_search(Id, Index, Type, Str) of
        {ok, Num, Hits} ->
            Data = lists:map(
                fun(#{<<"_id">>:=ObjId}=Hit) ->
                    FieldMap = maps:get(<<"_source">>, Hit, #{}),
                    maps:put(<<"_id">>, ObjId, FieldMap)
                end,
                Hits),
            {ok, Num, Data};
        {error, Error} ->
            {error, Error}
    end.




%% ===================================================================
%% Util
%% ===================================================================


spanish_ascii_analyzer() -> 
    #{
        analyzer => #{
            spanish_ascii => #{
                tokenizer => standard,
                filter => [
                    lowercase, 
                    asciifolding, 
                    spanish_stop, 
                    % spanish_keywords, 
                    spanish_stemmer]
            }
        },
        filter => #{
            spanish_stop => #{
                type => stop,
                stopwords => <<"_spanish_">>
            },
            % spanish_keywords => #{
            %     type => keyword_marker
            %     keywords => [] 
            % },
            spanish_stemmer => #{
                type => stemmer,
                language => light_spanish
            }
        }
    }.





%% ===================================================================
%% Internal
%% ===================================================================


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


% %% @private
% request_single(Id, Method, Path) ->
%     case request_data(Id, Method, Path, <<>>) of
%         {ok, [Term]} -> {ok, Term};
%         {error, Error} -> {error, Error}
%     end.


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


% %% @private
% split(Bin) ->
%     binary:split(Bin, <<" ">>, [global]).


% %% @private
% pre_index(all) -> [];
% pre_index(Index) -> [Index, "/"].

%% @private
post_index(all) -> [];
post_index(Index) -> ["/", Index].

%% @private
list_get_source(#{fields:=[<<"_all">>]}) ->
    <<"&_source=true">>;

list_get_source(#{fields:=Fields}) when is_list(Fields) ->
    <<"&_source_includes=", (nklib_util:bjoin(Fields))/binary>>;

list_get_source(_) ->
    <<"&_source=false">>.


%% @private
list_get_query([], []) ->
    [];

list_get_query([], Acc) ->
    [<<"&default_operator=AND&q=">>, Acc];

list_get_query([{Field, Value}|Rest], Acc) ->
    Term = <<"+", (to_bin(Field))/binary, $:, (to_bin(Value))/binary>>,
    list_get_query(Rest, [Term|Acc]).
   

%% @private
list_get_sort(#{sort_by:=Sort}=Opts) when is_list(Sort), length(Sort)>0 ->
    Order = to_bin(maps:get(sort_order, Opts, asc)),
    Changes = maps:get(sort_fields, Opts, #{}),
    Fields = lists:map(
        fun(Field) ->
            Field1 = to_bin(Field),
            Field2 = case maps:find(Field1, Changes) of
                {ok, Ch} -> to_bin(Ch);
                error -> Field1
            end,
            <<Field2/binary, $:, Order/binary>>
        end,
        Sort),
    [<<"&sort=", (nklib_util:bjoin(Fields))/binary>>];

list_get_sort(_) ->
    [].


%% @private
to_bin(Term) ->
    nklib_util:to_binary(Term).

%% @private
to_int(Term) ->
    nklib_util:to_integer(Term).
