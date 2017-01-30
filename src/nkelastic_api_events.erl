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

%% @doc NkELASTIC external events processing

-module(nkelastic_api_events).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([event/3]).

-include_lib("nkservice/include/nkservice.hrl").



%% ===================================================================
%% Callbacks
%% ===================================================================

%% @private
-spec event(nkelastic_session:id(), nkelastic_session:event(),
            nkelastic_session:session()) ->
	{ok, nkelastic_session:session()}.

event(_SessId, _Event, Session) ->
    {ok, Session}.



%% ===================================================================
%% Internal
%% ===================================================================


% %% @doc Sends an event
% -spec send_event(nkelastic_session:id(), nkservice_events:type(), 
%                  nkservice_events:body(), nkelastic_session:session()) ->
%     ok.

% %% @private
% send_event(SessId, Type, Body, #{srv_id:=SrvId}=Session) ->
%     Event = #event{
%         srv_id = SrvId,     
%         class = <<"media">>, 
%         subclass = <<"session">>,
%         type = nklib_util:to_binary(Type),
%         obj_id = SessId,
%         body = Body
%     },
%     send_direct_event(Event, Session),
%     nkservice_events:send(Event),
%     {ok, Session}.


% %% @private
% send_direct_event(#event{type=Type, body=Body}=Event, Session) ->
%     case Session of
%         #{session_events:=Events, user_session:=ConnId} ->
%             case lists:member(Type, Events) of
%                 true ->
%                     Event2 = case Session of
%                         #{session_events_body:=Body2} ->
%                             Event#event{body=maps:merge(Body, Body2)};
%                         _ ->
%                             Event
%                     end,
%                     nkservice_api_server:event(ConnId, Event2);
%                 false ->
%                     ok
%             end;
%         _ ->
%             ok
%     end.

