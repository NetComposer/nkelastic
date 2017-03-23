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
-export_type([query/0, search_opts/0, search_sort_opts/0]).
-export([parse/2, syntax/0]).
-export([spanish_ascii_analyzer/0]).
-export([fun_syntax/3]).

%% ===================================================================
%% Types
%% ===================================================================

-type query() :: map().

-type search_opts() ::
    #{
        from => integer(),
        size => integer(),
        fields => [binary()],                            %% "_all" for all
        sort => atom() | binary() | #{atom()|binary() => search_sort_opts()},
        sort_fields_map => #{atom() | binary() => atom() | binary()},
        aggs => map()
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
-spec parse(map(), search_opts()) ->
    {ok, map()} | {error, term()}.

parse(Query, Opts) ->
    Meta = maps:with([sort_fields_map], Opts),
    case nklib_syntax:parse(Opts, syntax(), Meta) of
        {ok, Body1, _, _} ->
            Body2 = set_defaults_opts(Body1),
            Body3 = case is_map(Query) of
                true ->
                    Body2#{query=>Query};
                false ->
                    Body2
            end,
            Body4 = case Opts of
                #{aggs:=Aggs} when is_map(Aggs) ->
                    Body3#{aggs=>Aggs};
                _ ->
                    Body3
            end,
            lager:info("Search body: ~p", [Body4]),
            {ok, Body4};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
syntax() ->
    #{
        from => {integer, 0, none},
        size => {integer, 0, none},
        sort => fun ?MODULE:fun_syntax/3,
        fields => fun ?MODULE:fun_syntax/3,
        sort_fields_map => ignore,
        aggs => ignore,
        delete => ignore
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


%% @private
set_defaults_opts(Body) ->
    case Body of
        #{'_source':=_} ->
            Body;
        _ ->
            Body#{'_source'=>false}
    end.



%% @doc
fun_syntax(sort, Val, Meta) ->
    fun_syntax_sort(Val, Meta, []);

fun_syntax(fields, Val, _Meta) ->
    case nklib_syntax:spec({list, binary}, Val) of
        {ok, []} ->
            {ok, '_source', false};
        {ok, [<<"_all">>]} ->
            {ok, '_source', true};
        {ok, List} ->
            {ok, '_source', List};
        _ ->
            error
    end.


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
    Name = syntax_sort_map(to_bin(Key), Meta),
    fun_syntax_sort(Rest, Meta, [Name|Acc]);

fun_syntax_sort([Key|Rest], Meta, Acc) when is_list(Key); is_integer(hd(Key)) ->
    Name = syntax_sort_map(to_bin(Key), Meta),
    fun_syntax_sort(Rest, Meta, [Name|Acc]);

fun_syntax_sort(_Other, _Meta, _Acc) ->
    error.


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


%% @private
to_bin(T) -> nklib_util:to_binary(T).