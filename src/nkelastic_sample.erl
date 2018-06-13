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

%% @doc 
-module(nkelastic_sample).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-define(SRV, es_test).

-compile(export_all).
-compile(nowarn_export_all).


-include_lib("nkservice/include/nkservice.hrl").

%% ===================================================================
%% Public
%% ===================================================================



%% @doc Starts the service
start() ->
    Spec = #{
        plugins => [?MODULE],
        packages => [
            #{
                id => es1,
                class => 'Elastic',
                config => #{
                    targets => [
                        #{
                            url => "http://127.0.0.1:9200",
                            debug => true
                        }
                    ],
                    debug => pooler,
                    index => i1,
                    type => t1
                }
            }
        ],
        modules => [
            #{
                id => s1,
                class => luerl,
                code => s1(),
                debug => true
            }
        ]
    },
    nkservice:start(?SRV, Spec).


%% @doc Stops the service
stop() ->
    nkservice:stop(?SRV).


getRequest() ->
    nkservice_luerl_instance:call({?SRV, s1, main}, [getRequest], []).


s1() -> <<"
    esConfig = {
        targets = {
            {
                url = 'http://127.0.0.1:9200',
                weight = 100,
                pool = 5
            }
        },
        resolveInterval = 0,
        debug = {'full', 'pooler'}
    }

    es = startPackage('Elastic', esConfig)

    function getRequest()
        return es.request('get', '/')
    end

">>.


opts() ->
    nkservice_util:get_cache(?SRV, nkelastic, <<"es1">>, opts).

