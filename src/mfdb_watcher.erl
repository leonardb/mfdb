%%%-------------------------------------------------------------------
%%% @copyright (C) 2020, Leonard Boyce
%%% @doc
%%% Watches a specific key in a table and notifies subscribers
%%% of changes to the value
%%% @end
%%%-------------------------------------------------------------------
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

-record(watcher_state, {db, prefix :: binary(), table :: binary(), key :: binary(), mon, notifies = [], orig}).

-spec subscribe(ReplyType :: notify(), FromPidRef :: {pid(), reference()}, Prefix :: binary(), Key :: binary()) -> ok | {error, function_not_exported}.
subscribe(ReplyType, FromPidRef, Prefix, Key) ->
    gen_server:call(?VIA(mfdb_lib:encode_key(Prefix, Key)), {subscribe, ReplyType, FromPidRef}).

-spec unsubscribe(pid(), binary(), binary()) -> ok.
unsubscribe(Pid, Prefix, Key) ->
    gen_server:call(?VIA(mfdb_lib:encode_key(Prefix, Key)), {unsubscribe, Pid}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Table, Prefix, Key) ->
    lager:info("starting: ~p", [?VIA(mfdb_lib:encode_key(Prefix, Key))]),
    gen_server:start_link(?VIA(mfdb_lib:encode_key(Prefix, Key)), ?MODULE, [Table, Prefix, Key], []).

init([Table, Prefix, Key]) ->
    process_flag(trap_exit, true),
    [#conn{} = Conn] = ets:lookup(?MODULE, conn),
    Db = mfdb_conn:connection(Conn),
    EncKey = mfdb_lib:encode_key(Prefix, Key),
    OVal = case erlfdb:get(Db, EncKey) of
               not_found -> undefined;
               EncVal -> mfdb_lib:decode_val(Db, Prefix, EncVal)
           end,
    Mon = spawn_watcher(Db, Prefix, Key),
    {ok, #watcher_state{db = Db, table = Table, prefix = Prefix,
                        key = Key, notifies = [],
                        mon = Mon, orig = OVal}}.

handle_call({subscribe, {notify, NotifyType}, {Pid, _Ref}}, _From,
            #watcher_state{key = Key, notifies = Notifies0} = State) ->
    lager:info("Subscribing ~p to monitor on key ~p", [Pid, Key]),
    Notifies = case lists:keytake(Pid, 2, Notifies0) of
                   false ->
                       %% Monitor the new subscriber
                       Ref = monitor(process, Pid),
                       lists:usort([{notify, NotifyType, Pid, Ref} | Notifies0]);
                   {value, {_OMethod, Pid, ORef}, Notifies1} ->
                       %% Replace the notification method for a pre-existing subscription
                       demonitor(ORef),
                       Ref = monitor(process, Pid),
                       lists:usort([{notify, NotifyType, Pid, Ref} | Notifies1])
               end,
    {reply, ok, State#watcher_state{notifies = Notifies}};
handle_call({subscribe, {callback, Module, Function}, {Pid, _Ref}}, _From,
            #watcher_state{key = Key, notifies = Notifies0} = State) ->
    %% Callback must be an exported 4-arity function
    case erlang:function_exported(Module, Function, 4) of
        true ->
            lager:info("Adding Callback for ~p with monitor on key ~p", [Pid, Key]),
            Notifies = lists:usort([{callback, Module, Function} | Notifies0]),
            {reply, ok, State#watcher_state{notifies = Notifies}};
        false ->
            {reply, {error, function_not_exported}, State}
    end;
handle_call({unsubscribe, {Pid, _Ref}}, _From,
            #watcher_state{key = Key, notifies = Notifies0} = State) ->
    lager:info("Unsubscribing ~p from monitor on key ~p", [Pid, Key]),
    Notifies = case lists:keytake(Pid, 2, Notifies0) of
                   false ->
                       Notifies0;
                   {value, {_, Pid, Ref}, Notifies1} ->
                       demonitor(Ref),
                       Notifies1
               end,
    {reply, ok, State#watcher_state{notifies = Notifies}};
handle_call(stop, _From,
            #watcher_state{key = Key, mon = {Pid, Ref}, notifies = Notifies} = State) ->
    lager:info("Stopping monitor on key ~p", [Key]),
    [demonitor(NRef) || {notify, _, _Pid, NRef} <- Notifies],
    demonitor(Ref),
    exit(Pid, kill),
    {stop, normal, ok, State};
handle_call(_Request, _From, #watcher_state{} = State) ->
    {reply, ok, State}.

handle_cast(updated, #watcher_state{db = Db, table = Table,
                                    prefix = Prefix, key = Key,
                                    notifies = Watchers, orig = Orig} = State) ->
    EncKey = mfdb_lib:encode_key(Prefix, Key),
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
    lager:error("Unknown cast ~p", [_Request]),
    {noreply, State}.

handle_info({'DOWN', Ref, process, Pid, Resp},
            #watcher_state{db = Db, prefix = Prefix, key = Key, mon = {Pid, Ref}} = State) ->
    lager:warning("Watcher for ~p went down: ~p", [Key, Resp]),
    demonitor(Ref),
    lager:info("Starting watcher for ~p ~p", [Db, mfdb_lib:encode_key(Prefix, Key)]),
    Mon = spawn_watcher(Db, Prefix, Key),
    {noreply, State#watcher_state{mon = Mon}};
handle_info({'EXIT', Pid, Msg},
            #watcher_state{db = Db, prefix = Prefix, key = Key, mon = {Pid, Ref}} = State) ->
    lager:error("Watcher for ~p Died: ~p / ~p", [Key, Msg]),
    demonitor(Ref),
    Mon = spawn_watcher(Db, Prefix, Key),
    {noreply, State#watcher_state{mon = Mon}};
handle_info({'EXIT', Pid, _Msg}, #watcher_state{notifies = Notify0} = State) ->
    %% Subscriber died so demonitor
    Notify = case lists:keytake(Pid, 2, Notify0) of
                 false ->
                     Notify0;
                 {value, {_, Pid, Ref}, Notify1} ->
                     demonitor(Ref),
                     Notify1
             end,
    {noreply, State#watcher_state{notifies = Notify}};
handle_info({'DOWN', _Ref, process, Pid, _Resp}, #watcher_state{notifies = Notify0} = State) ->
    %% Subscriber died so demonitor
    Notify = case lists:keytake(Pid, 2, Notify0) of
                 false ->
                     Notify0;
                 {value, {_, Pid, Ref}, Notify1} ->
                     demonitor(Ref),
                     Notify1
             end,
    {noreply, State#watcher_state{notifies = Notify}};
handle_info(_Info, #watcher_state{} = State) ->
    {noreply, State}.

terminate(_Reason, #watcher_state{mon = {_Pid, Ref}} = _State) ->
    demonitor(Ref),
    lager:info("Terminating: ~p", [_Reason]),
    ok.

code_change(_OldVsn, #watcher_state{} = State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
spawn_watcher(Db, Prefix, Key) ->
    lager:info("Spawning watcher for ~p ~p on db ~p", [Key, Prefix, Db]),
    spawn_monitor(fun() -> watcher_(Db, Prefix, Key) end).

%% @doc once a watcher receives a message it must re-watch the key
watcher_(Db, Prefix, Key) ->
    WatchKey = mfdb_lib:encode_key(Prefix, Key),
    Future = erlfdb:watch(Db, WatchKey),
    case erlfdb:wait(Future, [{timeout, infinity}]) of
        ok ->
            gen_server:cast(?VIA(WatchKey), updated),
            watcher_(Db, Prefix, Key);
        Err ->
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
    try Mod:Fun(Table, Key, Action, Value)
    catch
        E:M:St ->
            lager:error("Callback to ~p:~p/4 failed: ~p",
                        [Mod, Fun, {E,M, St}])
    end,
    notify_(Rest, Val);
notify_([{notify, call, Pid, _Ref} | Rest], Val) ->
    try gen_server:call(Pid, Val)
    catch
        E:M:St ->
            lager:error("call to Pid ~p failed: ~p",
                        [Pid, {E,M, St}])
    end,
    notify_(Rest, Val);
notify_([{notify, cast, Pid, _Ref} | Rest], Val) ->
    try gen_server:cast(Pid, Val)
    catch
        E:M:St ->
            lager:error("cast to Pid ~p failed: ~p",
                        [Pid, {E,M, St}])
    end,
    notify_(Rest, Val);
notify_([{notify, info, Pid, _Ref} | Rest], Val) ->
    try Pid ! Val
    catch
        E:M:St ->
            lager:error("notify to Pid ~p failed: ~p",
                        [Pid, {E,M, St}])
    end,
    notify_(Rest, Val).
