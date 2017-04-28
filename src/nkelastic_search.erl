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

%% @doc NkELASTIC search utilities

-module(nkelastic_search).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export_type([query/0, search_spec/0, search_sort_opts/0]).
-export([query/1]).
-export([spanish_ascii_analyzer/0]).
-export([fun_syntax/3]).
-export([test/0]).

%% ===================================================================
%% Types
%% ===================================================================

-type query() :: map().


%% Filters:
%% ">..."
%% ">=..."
%% "<..."
%% "<=..."
%% "<...-...>"
%% "<>"
%% "!..."
%%
%% Sort fields can be "asc:...", "desc:..."

-type search_spec() ::
    #{
        from => integer(),
        size => integer(),
        fields => [binary()],
        filters => #{atom()|binary() => term()},         %% See above
        sort => atom() | binary() | #{atom()|binary() => search_sort_opts()},
        simply_query => binary(),
        sort_fields_map => #{atom() | binary() => atom() | binary()}
%%        aggs => map()
    }.


-type search_sort_opts() ::
    #{
        order => asc | desc,
        mode => min | max | sum | avg | median,
        missing => binary()                             %% _last, _first
    }.





%% ===================================================================
%% Public
%% ===================================================================


%% @doc
-spec query(search_spec()) ->
    {ok, map()} | {error, term()}.

query(Spec) ->
    Meta = maps:with([sort_fields_map], Spec),
    Syntax1 = syntax(),
    Syntax2 = Syntax1#{sort_fields_map => ignore, aggs => ignore},
    case nklib_syntax:parse(Spec, Syntax2, Meta) of
        {ok, Body1, _, _} ->
            Body2 = case maps:is_key('_source', Body1) of
                true ->
                    Body1;
                false ->
                    Body1#{'_source' => true}
            end,
            Query1 = case maps:find(simple_query, Body2) of
                {ok, SQ} ->
                    #{must => #{simple_query_string => #{query => SQ}}};
                error ->
                    #{}
            end,
            Query2 = case maps:get(filters, Body2, []) of
                [] ->
                    Query1;
                Filters ->
                    Query1#{filter => Filters}
            end,
            Body3 = case map_size(Query2) of
                0 ->
                    Body2;
                _ ->
                    #{query => #{bool => Query2}}
            end,
            Body4 = case Spec of
                #{aggs:=Aggs} when is_map(Aggs) ->
                    Body3#{aggs=>Aggs};
                _ ->
                    Body3
            end,
            Body5 = maps:without([filters, simple_query], Body4),
            %% lager:info("Query: ~s", [nklib_json:encode_pretty(Body5)]),
            {ok, Body5};
        {error, Error} ->
            {error, Error}
    end.



%% @private
syntax() ->
    #{
        from => {integer, 0, none},
        size => {integer, 0, none},
        sort => fun ?MODULE:fun_syntax/3,
        fields => fun ?MODULE:fun_syntax/3,
        filters => fun ?MODULE:fun_syntax/3,
        simple_query => binary
    }.


%% ===================================================================
%% Analyzers
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



%% @doc
fun_syntax(sort, Val, Meta) ->
    fun_syntax_sort(Val, Meta, []);

fun_syntax(fields, Val, _Meta) ->
    case nklib_syntax:parse([{fields, Val}], #{fields=>{list, binary}}) of
        {ok, [], _, _} ->
            {ok, '_source', false};
%%        {ok, #{fields:=[<<"_all">>]}, _, _} ->
%%            {ok, '_source', true};
        {ok, #{fields:=[]}, _, _} ->
            {ok, '_source', false};
        {ok, #{fields:=Fields}, _, _} ->
            {ok, '_source', Fields};
        _ ->
            error
    end;

fun_syntax(filters, Map, _Meta) when is_map(Map) ->
    fun_syntax_filters(maps:to_list(Map), []);

fun_syntax(filters, _Val, _Meta) ->
    error.


%%  ----  Sort  ------------------

%% @private
sort_syntax() ->
    #{
        order => {atom, [asc, desc]},
        mode => {atom, [min, max, sum, avg, median]},
        missing => binary       %% _last, _first
    }.

%% @private
fun_syntax_sort([], _Meta, Acc) ->
    {ok, lists:reverse(Acc)};

fun_syntax_sort([Map|Rest], Meta, Acc) when is_map(Map) ->
    case maps:to_list(Map) of
        [{Field, Data}] ->
            Syntax = sort_syntax(),
            case nklib_syntax:parse(Data, Syntax) of
                {ok, _, _, [UnkField|_]} ->
                    {error, {syntax_error, <<"sort.", UnkField/binary>>}};
                {ok, Parsed, _, []} ->
                    Name = syntax_sort_map(to_bin(Field), Meta),
                    fun_syntax_sort(Rest, Meta, [#{Name=>Parsed}|Acc]);
                {error, _} ->
                    error
            end;
        _ ->
            error
    end;

fun_syntax_sort([Key|Rest], Meta, Acc) when is_binary(Key); is_atom(Key) ->
    Acc2 = case to_bin(Key) of
        <<"asc:", Key2/binary>> ->
            Name = syntax_sort_map(to_bin(Key2), Meta),
            [#{Name => #{order=>asc}}|Acc];
        <<"desc:", Key2/binary>> ->
            Name = syntax_sort_map(to_bin(Key2), Meta),
            [#{Name => #{order=>desc}}|Acc];
        Key2 ->
            Name = syntax_sort_map(to_bin(Key2), Meta),
            [Name|Acc]
    end,
    fun_syntax_sort(Rest, Meta, Acc2);

fun_syntax_sort([Key|Rest], Meta, Acc) when is_list(Key); is_integer(hd(Key)) ->
    fun_syntax_sort([to_bin(Key)|Rest], Meta, Acc);

fun_syntax_sort([_|_], _Meta, _Acc) ->
    error;

fun_syntax_sort(Other, Meta, Acc) ->
    fun_syntax_sort([Other], Meta, Acc).


%% @private
syntax_sort_map(Name, #{sort_fields_map:=Mappings}) ->
    case maps:find(Name, Mappings) of
        {ok, Name2} ->
            to_bin(Name2);
        error ->
            case catch binary_to_existing_atom(Name, utf8) of
                {'EXIT', _} ->
                    Name;
                Atom ->
                    case maps:find(Atom, Mappings) of
                        {ok, Name2} ->
                            to_bin(Name2);
                        error ->
                            Name
                    end
            end
    end;

syntax_sort_map(Name, _Meta) ->
    Name.


%%  ----  Fields  ------------------


%% @private
fun_syntax_filters([], Acc) ->
    {ok, Acc};

fun_syntax_filters([{Field, Val}|Rest], Acc) ->
    Filter = fun_syntax_get_filter(to_bin(Field), Val),
    fun_syntax_filters(Rest, [Filter|Acc]).

%% @private
fun_syntax_get_filter(Field, <<"childs_of:/">>) ->
    #{wildcard => #{Field => <<"/?*">>}};
fun_syntax_get_filter(Field, <<"childs_of:", Data/binary>>) ->
    #{prefix => #{Field => <<Data/binary, $/>>}};
fun_syntax_get_filter(Field, <<"prefix:", Data/binary>>) ->
    #{prefix => #{Field => <<Data/binary>>}};
fun_syntax_get_filter(Field, <<">=", Data/binary>>) ->
    #{range => #{Field => #{gte => Data}}};
fun_syntax_get_filter(Field, <<">", Data/binary>>) ->
    #{range => #{Field => #{gt => Data}}};
fun_syntax_get_filter(Field, <<"<=", Data/binary>>) ->
    #{range => #{Field => #{lte => Data}}};
fun_syntax_get_filter(Field, <<"<", Data/binary>>) ->
    case binary:at(Data, byte_size(Data)-1) of
        $> ->
            case binary:split(Data, <<"-">>) of
                [Data1, Data2] ->
                    #{range => #{Field => #{gte=>Data1, lte=>Data2}}};
                _ ->
                    #{range => #{Field => #{lt => Data}}}
            end;
        _ ->
            #{range => #{Field => #{lt => Data}}}
    end;
fun_syntax_get_filter(Field, <<"!", Data/binary>>) ->
    #{bool => #{must_not => #{term => #{Field => Data}}}};
fun_syntax_get_filter(Field, Values) when is_list(Values)->
    #{terms => #{Field => Values}};
fun_syntax_get_filter(Field, Value) ->
    #{term => #{Field => Value}}.


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).


%% @private
test() ->
    Spec = #{
        from => 1,
        fields => a,
        sort => [b,#{c=>#{order=>asc}}, <<"asc:n1">>, <<"desc:n2">>],
        filters => #{a=>1, b=><<">a">>, c=><<">=a">>, d=><<"<a">>, e=><<"<=a">>, f=><<"<a-b>">>, g=>[1,a],
                     h=><<"!b">>, i=><<"childs_of:/">>, j=><<"childs_of:/a/b">>, k=><<"prefix:pp">>}
    },
    query(Spec).
