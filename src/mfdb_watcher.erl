%%% @copyright 2020 Leonard Boyce <leonard.boyce@lucidlayer.com>
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%%% use this file except in compliance with the License. You may obtain a copy of
%%% the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%%% License for the specific language governing permissions and limitations under
%%% the License.
-module(mfdb_watcher).

-behaviour(gen_server).

-export([subscribe/4,
         unsubscribe/3]).

-export([start_link/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-define(VIA(K), {via, gproc, {n, l, {mfdb_watcher, K}}}).

-include("mfdb.hrl").

-type notify_module() :: atom().
-type notify_function() :: atom().

-type notify_callback() :: {callback, notify_module(), notify_function()}.
-type notify_process() :: {notify, call | cast | info}.
-type notify() :: notify_process() | notify_callback().

-record(watcher_state, {db, pfx :: binary(), table :: binary(), key :: binary(), mon, notifies = [], orig}).

-spec subscribe(ReplyType :: notify(), FromPidRef :: {pid(), reference()}, Prefix :: binary(), Key :: binary()) -> ok | {error, function_not_exported}.
subscribe(ReplyType, FromPidRef, TblPfx, Key) ->
    K = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, Key}),
    gen_server:call(?VIA(K), {subscribe, ReplyType, FromPidRef}).

-spec unsubscribe(pid(), binary(), binary()) -> ok.
unsubscribe(Pid, TblPfx, Key) ->
    K = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, Key}),
    gen_server:call(?VIA(K), {unsubscribe, Pid}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Table, TblPfx, Key) ->
    K = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, Key}),
    gen_server:start_link(?VIA(K), ?MODULE, [Table, TblPfx, Key], []).

init([Table, TblPfx, Key]) ->
    process_flag(trap_exit, true),
    [#conn{} = Conn] = ets:lookup(mfdb_manager, conn),
    Db = mfdb_conn:connection(Conn),
    EncKey = mfdb_lib:encode_key(TblPfx, {?DATA_PREFIX, Key}),
    OVal = case erlfdb:get(Db, EncKey) of
               not_found -> undefined;
               EncVal -> mfdb_lib:decode_val(Db, TblPfx, EncVal)
           end,
    Mon = spawn_watcher(Db, TblPfx, Key),
    {ok, #watcher_state{db = Db, table = Table, pfx = TblPfx,
                        key = Key, notifies = [],
                        mon = Mon, orig = OVal}}.

handle_call({subscribe, {notify, NotifyType}, {Pid, _Ref}}, _From,
            #watcher_state{notifies = Notifies0} = State) ->
    Notifies = case lists:keytake(Pid, 3, Notifies0) of
                   false ->
                       %% Monitor the new subscriber
                       Ref = monitor(process, Pid),
                       lists:usort([{notify, NotifyType, Pid, Ref} | Notifies0]);
                   {value, {notify, _OMethod, Pid, ORef}, Notifies1} ->
                       %% Replace the notification method for a pre-existing subscription
                       demonitor(ORef),
                       Ref = monitor(process, Pid),
                       lists:usort([{notify, NotifyType, Pid, Ref} | Notifies1])
               end,
    {reply, ok, State#watcher_state{notifies = Notifies}};
handle_call({subscribe, {callback, Module, Function}, _PidRef}, _From,
            #watcher_state{notifies = Notifies0} = State) ->
    %% Callback must be an exported 4-arity function
    case erlang:function_exported(Module, Function, 4) of
        true ->
            Notifies = lists:usort([{callback, Module, Function} | Notifies0]),
            {reply, ok, State#watcher_state{notifies = Notifies}};
        false ->
            {reply, {error, function_not_exported}, State}
    end;
handle_call({unsubscribe, {Pid, _Ref}}, _From,
            #watcher_state{mon = {WatcherPid, WatcherRef}, notifies = Notifies0} = State) ->
    case remove_notify(Pid, Notifies0) of
        [] ->
            %% Nothing is waiting to be notified,
            %% so stop watching
            demonitor(WatcherRef),
            exit(WatcherPid, kill),
            {stop, normal, ok, State#watcher_state{notifies = []}};
        Notifies ->
            {reply, ok, State#watcher_state{notifies = Notifies}}
    end;
handle_call(stop, _From,
            #watcher_state{mon = {Pid, Ref}, notifies = Notifies} = State) ->
    [demonitor(NRef) || {notify, _, _Pid, NRef} <- Notifies],
    demonitor(Ref),
    exit(Pid, kill),
    {stop, normal, ok, State};
handle_call(_Request, _From, #watcher_state{} = State) ->
    {reply, ok, State}.

handle_cast(updated, #watcher_state{db = Db, table = Tab0,
                                    pfx = Prefix, key = Key,
                                    notifies = Watchers, orig = Orig} = State) ->
    EncKey = mfdb_lib:encode_key(Prefix, {?DATA_PREFIX, Key}),
    Table = binary_to_existing_atom(Tab0),
    NState = case erlfdb:get(Db, EncKey) of
                 not_found ->
                     %% Key was touched and no longer exists
                     notify_(Watchers, {Table, Key, deleted}),
                     State#watcher_state{orig = undefined};
                 EncVal when Orig =:= undefined ->
                     %% Key was created
                     Val = mfdb_lib:decode_val(Db, Prefix, EncVal),
                     notify_(Watchers, {Table, Key, created, Val}),
                     State#watcher_state{orig = Val};
                 EncVal ->
                     case mfdb_lib:decode_val(Db, Prefix, EncVal) of
                         Orig ->
                             %% Value for key is unchanged
                             State;
                         Val ->
                             notify_(Watchers, {Table, Key, updated, Val}),
                             State#watcher_state{orig = Val}
                     end
             end,
    {noreply, NState};
handle_cast(_Request, #watcher_state{} = State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, _Resp}, #watcher_state{db = Db, pfx = Prefix,
                                                               key = Key, mon = {Pid, Ref}
                                                              } = State) ->
    demonitor(Ref),
    Mon = spawn_watcher(Db, Prefix, Key),
    {noreply, State#watcher_state{mon = Mon}};
handle_info({'EXIT', Pid, _Msg}, #watcher_state{db = Db, pfx = Prefix,
                                                key = Key, mon = {Pid, Ref}
                                               } = State) ->
    demonitor(Ref),
    Mon = spawn_watcher(Db, Prefix, Key),
    {noreply, State#watcher_state{mon = Mon}};
handle_info({'EXIT', Pid, _Msg}, #watcher_state{notifies = Notifies0} = State) ->
    %% Subscriber died so demonitor and maybe shut down watcher
    Notifies = remove_notify(Pid, Notifies0),
    maybe_stop(Notifies, State);
handle_info({'DOWN', _Ref, process, Pid, _Resp}, #watcher_state{notifies = Notifies0} = State) ->
    %% Subscriber died so demonitor and maybe shut down watcher
    Notifies = remove_notify(Pid, Notifies0),
    maybe_stop(Notifies, State);
handle_info(_Info, #watcher_state{} = State) ->
    {noreply, State}.

terminate(_Reason, #watcher_state{mon = {_Pid, Ref}} = _State) ->
    demonitor(Ref),
    ok.

code_change(_OldVsn, #watcher_state{} = State, _Extra) ->
    {ok, State}.

maybe_stop([], #watcher_state{mon = {WatcherPid, WatcherRef}} = State) ->
    %% Nothing is waiting to be notified,
    %% so stop watching
    demonitor(WatcherRef),
    exit(WatcherPid, kill),
    {stop, normal, State#watcher_state{notifies = []}};
maybe_stop(Notifies, State) ->
    {noreply, State#watcher_state{notifies = Notifies}}.

remove_notify(Pid, Notify0) ->
    case lists:keytake(Pid, 3, Notify0) of
        false ->
            Notify0;
        {value, {notify, _, Pid, Ref}, Notify1} ->
            demonitor(Ref),
            Notify1
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
spawn_watcher(Db, Prefix, Key) ->
    spawn_monitor(fun() -> watcher_(Db, Prefix, Key) end).

%% @doc once a watcher receives a message it must re-watch the key
watcher_(Db, Prefix, Key) ->
    WatchKey = mfdb_lib:encode_key(Prefix, {?DATA_PREFIX, Key}),
    Future = erlfdb:watch(Db, WatchKey),
    case erlfdb:wait(Future, [{timeout, infinity}]) of
        ok ->
            gen_server:cast(?VIA(WatchKey), updated),
            watcher_(Db, Prefix, Key);
        Err ->
            error_logger:error_msg("Watcher FDB error: ~p", [Err]),
            exit({error, Err})
    end.

notify_([], _Val) ->
    ok;
notify_([{callback, Mod, Fun} | Rest], Val) ->
    {Table, Key, Action, Value} =
        case Val of
            {T, K, deleted} ->
                {T, K, deleted, undefined};
            {T, K, A, V} ->
                {T, K, A, V}
        end,
    try Mod:Fun(binary_to_existing_atom(Table), Key, Action, Value)
    catch
        E:M:St ->
            error_logger:error_msg("Callback to ~p:~p/4 failed: ~p",
                                   [Mod, Fun, {E,M, St}])
    end,
    notify_(Rest, Val);
notify_([{notify, call, Pid, _Ref} | Rest], Val) ->
    try gen_server:call(Pid, Val)
    catch
        E:M:St ->
            error_logger:error_msg("call to Pid ~p failed: ~p",
                                   [Pid, {E,M, St}])
    end,
    notify_(Rest, Val);
notify_([{notify, cast, Pid, _Ref} | Rest], Val) ->
    try gen_server:cast(Pid, Val)
    catch
        E:M:St ->
            error_logger:error_msg("cast to Pid ~p failed: ~p",
                                   [Pid, {E,M, St}])
    end,
    notify_(Rest, Val);
notify_([{notify, info, Pid, _Ref} | Rest], Val) ->
    try Pid ! Val
    catch
        E:M:St ->
            error_logger:error_msg("notify to Pid ~p failed: ~p",
                                   [Pid, {E,M, St}])
    end,
    notify_(Rest, Val).
