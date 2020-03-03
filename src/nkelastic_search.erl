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
-export([syntax/0, fun_syntax/3]).
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
%% "<<...-...>>"
%% "!..."
%%
%% Sort fields can be "asc:...", "desc:..."

-type filter_field() :: atom() | binary().

-type filter_op() :: eq | values | gt | gte | lt | lte | prefix | subdir | exists | fuzzy | {fuzzy, map()} | date_range | query_string.

-type simple_query_opts() ::
    #{
        default_field => filter_field(),
        default_operator => 'AND' | 'OR'
    }.

-type filter_spec() ::
    {filter_field(), filter_op(), Val::term()} |
    {simple_query, Query::binary(), simple_query_opts()}.

-type filter_list() :: [filter_spec() | {'and'|'or'|'not', filter_spec()|[filter_spec()]}].

-type sort_spec() :: atom() | binary() | #{atom()|binary() => search_sort_opts()}.


-type search_spec() ::
    #{
        from => integer(),
        size => integer(),
        fields => [binary()],
        filters => #{atom()|binary() => term()}, %% See above
        filter_list => filter_list(),
        sort => sort_spec(),
        simply_query => binary(),
        simple_query_opts => #{fields=>[binary()], default_operator=>'AND' | 'OR'},
        sort_fields_map => #{atom() | binary() => atom() | binary()}
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
        {ok, #{filter_list:=FilterList}=Parsed, _} ->
            Body1 = maps:with([from, size, fields, sort, '_source'], Parsed),
            % A specific '_source' can be added by syntax fun. If not, add it
            Body2 = case maps:is_key('_source', Body1) of
                true ->
                    Body1;
                false ->
                    Body1#{'_source' => true}
            end,
            Body3 = case FilterList  of
                [] ->
                    Body2;
                _ ->
                    % We must use a compound query, best suited is 'bool'
                    % We use the 'filter' part of bool to set 'filter context'
                    % We use another compound now that we are in filter context
                    Body2#{query => #{bool => #{filter => #{bool => FilterList}}}}
            end,
            Body4 = case Spec of
                #{aggs:=Aggs} when is_map(Aggs) ->
                    Body3#{aggs=>Aggs};
                _ ->
                    Body3
            end,
            % lager:info("NEW Query: ~s", [nklib_json:encode_pretty(Body4)]),
            {ok, Body4};
        {ok, Parsed, _} ->
            Body1 = maps:with([from, size, fields, sort, '_source'], Parsed),
            % A specific '_source' can be added by syntax fun. If not, add it
            Body2 = case maps:is_key('_source', Body1) of
                true ->
                    Body1;
                false ->
                    Body1#{'_source' => true}
            end,
            Query1 = case maps:find(simple_query, Parsed) of
                {ok, SQ} ->
                    SQOpts = maps:get(simple_query_opts, Body2, #{}),
                    #{must => #{simple_query_string => SQOpts#{query => SQ}}};
                error ->
                    #{}
            end,
            % TODO Temporary hack
            Query2 = case Spec of
                #{get_deleted := true} ->
                    Query1;
                _ ->
                    Query1#{must_not => #{term => #{is_deleted=>true}}}
            end,
            Query3 = case maps:get(filters, Parsed, []) of
                [] ->
                    Query2;
                Filters ->
                    Query2#{filter=>Filters}
            end,
            Body3 = Body2#{query => #{bool =>Query3}},
            Body4 = case Spec of
                #{aggs:=Aggs} when is_map(Aggs) ->
                    Body3#{aggs=>Aggs};
                _ ->
                    Body3
            end,
            % lager:notice("Query: ~s", [nklib_json:encode_pretty(Body4)]),
            {ok, Body4};
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
        filter_list => fun ?MODULE:fun_syntax/3,
        simple_query => binary,
        simple_query_opts => #{
            fields => {list, binary},
            default_operator => {atom, ['OR', 'AND']}
        }
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
        {ok, [], _} ->
            {ok, '_source', false};
        {ok, #{fields:=[]}, _} ->
            {ok, '_source', false};
        {ok, #{fields:=Fields}, _} ->
            {ok, '_source', Fields};
        _ ->
            error
    end;

fun_syntax(filters, Map, _Meta) when is_map(Map) ->
    fun_syntax_filters(maps:to_list(Map), []);

fun_syntax(filter_list, List, _Meta) when is_list(List) ->
    case fun_syntax_filter_list(filter, List, #{}) of
        Map when is_map(Map) ->
            {ok, Map};
        error ->
            error;
        {error, Reason} ->
            {error, Reason}
    end;

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
                {ok, _, [UnkField|_]} ->
                    {error, {syntax_error, <<"sort.", UnkField/binary>>}};
                {ok, Parsed, []} ->
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
    Filter = fun_syntax_get_filter(Field, Val),
    fun_syntax_filters(Rest, [Filter|Acc]).

%% @private
fun_syntax_get_filter(Field, <<"childs_of:/">>) ->
    #{wildcard => #{Field => <<"/?*">>}};
fun_syntax_get_filter(Field, <<"childs_of:", Data/binary>>) ->
    #{prefix => #{Field => <<Data/binary, $/>>}};
fun_syntax_get_filter(Field, <<"prefix:", Data/binary>>) ->
    #{prefix => #{Field => <<Data/binary>>}};
fun_syntax_get_filter(Field, <<"query_string:", Data/binary>>) ->
    #{query_string => #{default_field => Field, 'query' => <<Data/binary>>}};
fun_syntax_get_filter(Field, <<">=", Data/binary>>) ->
    #{range => #{Field => #{gte => term(Data)}}};
fun_syntax_get_filter(Field, <<">", Data/binary>>) ->
    #{range => #{Field => #{gt => term(Data)}}};
fun_syntax_get_filter(Field, <<"<=", Data/binary>>) ->
    #{range => #{Field => #{lte => term(Data)}}};
fun_syntax_get_filter(Field, <<"<<", Data/binary>>) ->
    Size = byte_size(Data)-2,
    case Size>0 andalso binary:at(Data, Size) of
        $> ->
            case binary:split(<<Data:Size/binary>>, <<"-">>) of
                [Data1, Data2] ->
                    #{range => #{Field => #{gt=>term(Data1), lt=>term(Data2)}}};
                _ ->
                    #{}
            end;
        _ ->
            #{}
    end;
fun_syntax_get_filter(Field, <<"<", Data/binary>>) ->
    Size = byte_size(Data)-1,
    case Size>0 andalso binary:at(Data, Size) of
        $> ->
            case binary:split(<<Data:Size/binary>>, <<"-">>) of
                [Data1, Data2] ->
                    #{range => #{Field => #{gte=>term(Data1), lte=>term(Data2)}}};
                _ ->
                    #{range => #{Field => #{lt => term(Data)}}}
            end;
        _ ->
            #{range => #{Field => #{lt => term(Data)}}}
    end;
fun_syntax_get_filter(Field, <<"!", Data/binary>>) ->
    #{bool => #{must_not => #{term => #{Field => term(Data)}}}};
fun_syntax_get_filter(Field, Values) when is_list(Values) ->
    #{terms => #{Field => Values}};
fun_syntax_get_filter(Field, Value) ->
    #{term => #{Field => Value}}.



%% @private
fun_syntax_filter_list(_Ctx, [], Acc) ->
    Acc;

fun_syntax_filter_list(Ctx, [{Key, eq, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{term => #{f(Key) => Val}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, values, Val}|Rest], Acc) when is_list(Val) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{terms => #{f(Key) => Val}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, gt, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{range => #{f(Key) => #{gt => Val}}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, gte, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{range => #{f(Key) => #{gte => Val}}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, lt, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{range => #{f(Key) => #{lt => Val}}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, lte, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{range => #{f(Key) => #{lte => Val}}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, prefix, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{prefix => #{f(Key) => Val}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, fuzzy, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, [{f(Key), {fuzzy, #{}}, Val}|Rest], Acc);

fun_syntax_filter_list(Ctx, [{Key, {fuzzy, Opts}, Val}|Rest], Acc) ->
    Syntax = #{
        prefix_length => {integer, 0, 100},
        max_expansions => {integer, 1, 100},
        '__defaults' => #{
            prefix_length => 0,
            max_expansions => 50
        }
    },
    case nklib_syntax:parse(Opts, Syntax) of
        {ok, Opts2, _} ->
            Term = #{fuzzy => #{f(Key) => Opts2#{value=>Val}}},
            fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, Term, Acc));
        {error, Error} ->
            {error, Error}
    end;

fun_syntax_filter_list(Ctx, [{Key, date_range, Opts}|Rest], Acc) ->
    Syntax = #{
        period => {atom, [today, yesterday, this_week, last_week, last_7_days, this_month, last_month, this_year, last_year, custom]},
        start_of_week => {atom, [monday, tuesday, wednesday, thursday, friday, saturday, sunday]},
        time_zone => fun check_time_zone_syntax/1, %+/-HH:mm
        start_date => integer,
        end_date => integer,
        '__mandatory' => [period],
        '__defaults' => #{
            time_zone => <<"+00:00">>,
            start_of_week => sunday
        },
        '__post_check' => fun date_syntax_post_check/1
    },
    case nklib_syntax:parse(Opts, Syntax) of
        {ok, Opts2, _} ->
            Opts3 = get_date_search_fields(Opts2),
            Term = #{range => #{f(Key) => Opts3}},
            fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, Term, Acc));
        {error, Error} ->
            {error, Error}
    end;

fun_syntax_filter_list(Ctx, [{Key, query_string, Val}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{query_string => #{default_field => f(Key), 'query' => Val}}, Acc));

fun_syntax_filter_list(Ctx, [{Key, subdir, Path}|Rest], Acc) ->
    Term = case to_bin(Path) of
        <<"/">> ->
            #{wildcard => #{f(Key) => <<"/?*">>}};
        Path2 ->
            #{prefix => #{f(Key) => <<Path2/binary, $/>>}}
    end,
    fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, Term, Acc));

fun_syntax_filter_list(Ctx, [{Key, exists, Val}|Rest], Acc) ->
    case nklib_syntax:spec(boolean, Val) of
        {ok, true} ->
            fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{exists => #{field => f(Key)}}, Acc));
        {ok, false} when Ctx == filter ->
            fun_syntax_filter_list(Ctx, [{'not', {f(Key), exists, true}}|Rest], Acc);
        {ok, false} when Ctx == must_not ->
            fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, #{exists => #{field => f(Key)}}, Acc));
        {ok, false} when Ctx == should ->
            lager:error("NKLOG Used exists false in OR"),
            error;
        error ->
            error
    end;

fun_syntax_filter_list(Ctx, [{simple_query, Str}|Rest], Acc) ->
    fun_syntax_filter_list(Ctx, [{simple_query, Str, #{}}|Rest], Acc);

fun_syntax_filter_list(Ctx, [{simple_query, Str, Opts}|Rest], Acc) ->
    Syntax = #{
        default_field => binary,
        default_operator => {atom, ['OR', 'AND']},
        '__defaults' => #{
            default_field => <<"_all">>,
            default_operator => 'AND'
        }
    },
    case nklib_syntax:parse(Opts, Syntax) of
        {ok, Opts2, _} ->
            Term =  #{query_string => Opts2#{query => Str}},
            fun_syntax_filter_list(Ctx, Rest, add_filter(Ctx, Term, Acc));
        {error, Error} ->
            {error, Error}
    end;

fun_syntax_filter_list(filter, [{'not', List}|Rest], Acc) when is_list(List) ->
    Acc2 = fun_syntax_filter_list(must_not, List, Acc),
    fun_syntax_filter_list(filter, Rest, Acc2);

fun_syntax_filter_list(filter, [{'not', Op}|Rest], Acc) ->
    fun_syntax_filter_list(filter, [{'not', [Op]}|Rest], Acc);

fun_syntax_filter_list(filter, [{'and', List}|Rest], Acc) when is_list(List) ->
    fun_syntax_filter_list(filter, List++Rest, Acc);

fun_syntax_filter_list(filter, [{'and', Op}|Rest], Acc) ->
    fun_syntax_filter_list(filter, [{'and', [Op]}|Rest], Acc);

fun_syntax_filter_list(filter, [{'or', List}|Rest], Acc) when is_list(List) ->
    AccN = fun_syntax_filter_list(should, List, #{}),
    Should = maps:get(should, AccN, []),
    Must = maps:get(must, Acc, []),
    Must2 = [#{
        bool => #{
            should => Should
        }
    }|Must],
    Acc2 = Acc#{must => Must2},
    fun_syntax_filter_list(filter, Rest, Acc2);

fun_syntax_filter_list(filter, [{'or', Op}|Rest], Acc) ->
    fun_syntax_filter_list(filter, [{'or', [Op]}|Rest], Acc);

fun_syntax_filter_list(_Ctx, [_Other|_]=L, _Acc) ->
    lager:error("NKLOG Search Error (~p,~p,~p)", [_Ctx, L, _Acc]),
    error.


%% @private
add_filter(filter, Term, Acc) ->
    Filter1 = maps:get(filter, Acc, []),
    Filter2 = [Term|Filter1],
    Acc#{filter => Filter2};

add_filter(must_not, Term, Acc) ->
    Filter1 = maps:get(must_not, Acc, []),
    Filter2 = [Term|Filter1],
    Acc#{must_not => Filter2};

add_filter(should, Term, Acc) ->
    Filter1 = maps:get(should, Acc, []),
    Filter2 = [Term|Filter1],
    Acc#{should => Filter2}.



%% @private
f(Field) when is_atom(Field) -> to_bin(Field);
f(Field) when is_binary(Field) -> Field;
f(Field) when is_list(Field), is_integer(hd(Field)) -> to_bin(Field);
f(Field) when is_list(Field) -> list_to_binary(Field).


%% @private
date_syntax_post_check(Data) ->
    Map = maps:from_list(Data),
    case Map of
        #{period := custom, start_date := _, end_date := _} ->
            ok;
        #{period := custom, start_date := _} ->
            {error, {missing_field, end_date}};
        #{period := custom} ->
            {error, {missing_field, start_date}};
        #{period := _} ->
            ok;
        #{} ->
            ok
    end.


%% @private
check_time_zone_syntax(TimeZone) ->
    case do_parse_time_zone(to_bin(TimeZone)) of
        {ok, _} ->
            ok;
        _ ->
            {error, {field, time_zone}}
    end.


%% @private
do_parse_time_zone(Bin) ->
    case re:run(Bin, "^([\\+-])*(\\d+):(\\d+)$", [{capture, all_but_first, list}]) of
        {match, [Sign, H, M]} ->
            Factor = case Sign of
                [] -> -1;
                "+" -> -1;
                "-" -> 1
            end,
            H2 = list_to_integer(H),
            M2 = list_to_integer(M),
            {ok, Factor * ((H2*3600) + M2*60)};
        _ ->
            case re:run(Bin, "^([\\+-])*(\\d+)$", [{capture, all_but_first, list}]) of
                {match, [Sign, H]} ->
                    Factor = case Sign of
                        [] -> -1;
                        "+" -> -1;
                        "-" -> 1
                    end,
                    H2 = list_to_integer(H),
                    {ok, Factor * (H2*3600)};
                _ ->
                    error
            end
    end.


%% @private
unparse_time_with_format(Epoch, Zone, Format) ->
    Secs = round(Epoch / 1000),
    case do_parse_time_zone(Zone) of
        {ok, ZoneSecs} ->
            {{Y, M, D}, {H, Mi, _}} = nklib_util:timestamp_to_gmt(Secs - ZoneSecs),
            Day = case Format of
                <<"yyyy">> ->
                    list_to_binary(io_lib:format("~4..0B", [Y]));
                <<"yyyy-MM*">> ->
                    list_to_binary(io_lib:format("~4..0B-~2..0B*", [Y,M]));
                <<"yyyy-MM">> ->
                    list_to_binary(io_lib:format("~4..0B-~2..0B", [Y,M]));
                <<"yyyy-MM-dd">> ->
                    list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Y,M,D]));
                _ ->
                    list_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Y,M,D]))
            end,
            Hour = list_to_binary(io_lib:format("~2..0B:~2..0B", [H,Mi])),
            {ok, Day, Hour};
        {error, Error} ->
            {error, Error}
    end.


% {Interval, Format, From, Lower, To, Upper, TZOffset}
%% @private
get_date_search_fields(Spec) ->
    {_Interval, Format, From, Lower, To, Upper, TZOffset} = case Spec of
        #{period := custom, start_date := S, end_date := E, time_zone := TZ} ->
            I = get_interval(Spec),
            F = get_date_format(I),
            {ok, S2, _SH2} = unparse_time_with_format(S, TZ, F),
            {ok, E2, _EH2} = unparse_time_with_format(E, TZ, F),
            {I, F, S2, <<"gte">>, E2, <<"lte">>, TZ};
        #{period := today, time_zone := TZ} ->
            {<<"hour">>, <<"yyyy-MM-dd">>, <<"now/d">>, <<"gte">>, <<"now/h">>, <<"lte">>, TZ};
        #{period := yesterday, time_zone := TZ} ->
            {<<"hour">>, <<"yyyy-MM-dd">>, <<"now/d-1d">>, <<"gte">>, <<"now/d-1h">>, <<"lt">>, TZ};
        #{period := this_week, time_zone := TZ} ->
            O = to_bin(get_week_offset(Spec)),
            {<<"day">>, <<"yyyy-MM-dd">>, <<"now-", O/binary, "d/w+", O/binary, "d">>, <<"gte">>, <<"now+1w-", O/binary, "d/w+", O/binary, "d-1d">>, <<"lt">>, TZ};
        #{period := last_week, time_zone := TZ} ->
            O = to_bin(get_week_offset(Spec)),
            {<<"day">>, <<"yyyy-MM-dd">>, <<"now-", O/binary, "d/w+", O/binary, "d-w">>, <<"gte">>, <<"now-", O/binary, "d/w+", O/binary, "d-1d">>, <<"lt">>, TZ};
        #{period := last_7_days, time_zone := TZ} ->
            {<<"day">>, <<"yyyy-MM-dd">>, <<"now-6d/d">>, <<"gte">>, <<"now">>, <<"lte">>, TZ};
        #{period := this_month, time_zone := TZ} ->
            {<<"day">>, <<"yyyy-MM-dd">>, <<"now/M">>, <<"gte">>, <<"now">>, <<"lte">>, TZ};
        #{period := last_month, time_zone := TZ} ->
            {<<"day">>, <<"yyyy-MM-dd">>, <<"now-1M/M">>, <<"gte">>, <<"now/M-1d">>, <<"lt">>, TZ};
        #{period := this_year, time_zone := TZ} ->
            {<<"month">>, <<"yyyy-MM">>, <<"now/y">>, <<"gte">>, <<"now">>, <<"lte">>, TZ};
        #{period := last_year, time_zone := TZ} ->
            {<<"month">>, <<"yyyy-MM">>, <<"now-1y/y">>, <<"gte">>, <<"now/y-1d">>, <<"lt">>, TZ}
    end,
    #{
        Lower => From,
        Upper => To,
        format => Format,
        time_zone => TZOffset
    }.


%% @private
get_week_offset(Spec) ->
    Weekday = maps:get(start_of_week, Spec, sunday),
    case Weekday of
        monday    -> 0;
        tuesday   -> 1;
        wednesday -> 2;
        thursday  -> 3;
        friday    -> 4;
        saturday  -> 5;
        sunday    -> 6;
        _ ->
            lager:error("Invalid day submitted: ~p", [Weekday]),
            0
    end.


%% @private
get_interval(#{start_date := S, end_date := E}=_Spec) ->
    Diff = E - S,
    case Diff of
        N when N > 1000*60*60*24*365*5 ->
            <<"year">>;
        N when N > 1000*60*60*24*365*2 ->
            <<"quarter">>;
        N when N > 1000*60*60*24*30*2 ->
            <<"month">>;
        N when N > 1000*60*60*24*2 ->
            <<"day">>;
        _ ->
            <<"hour">>
    end.


% @private
get_date_format(Interval) ->
    case Interval of
        year ->
            <<"yyyy">>;
        quarter ->
            <<"yyyy-MM*">>;
        month ->
            <<"yyyy-MM">>;
        day ->
            <<"yyyy-MM-dd">>;
        hour ->
            <<"yyyy-MM-dd">>;
        _ ->
            <<"yyyy-MM-dd">>
    end.


%% @private
to_bin(T) when is_binary(T) -> T;
to_bin(T)                   -> nklib_util:to_binary(T).


%% @private
term(<<Ch, _/binary>> = Data) when Ch>=$0, Ch=<$9 ->
    case catch binary_to_integer(Data) of
        Num when is_integer(Num) -> Num;
        _ -> Data
    end;

term(Data) ->
    Data.


%% @private
test() ->
    Spec = #{
        from => 1,
        fields => a,
        sort => [b, #{c=>#{order=>asc}}, <<"asc:n1">>, <<"desc:n2">>],
        filters => #{a=>1, b=><<">a">>, c=><<">=a">>, d=><<"<a">>, e=><<"<=a">>, f=><<"<a-b>">>, g=>[1, a],
                     h=><<"!b">>, i=><<"childs_of:/">>, j=><<"childs_of:/a/b">>, k=><<"prefix:pp">>}
    },
    query(Spec).
