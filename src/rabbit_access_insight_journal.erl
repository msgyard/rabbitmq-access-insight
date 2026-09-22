%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% The local access journal: every record this node produces, in sequence.
%%
%% Records are appended to segment files, each an OTP disk_log named after
%% the first sequence number it holds (journal/<seq>.log). A segment is
%% closed once it reaches the segment size; the oldest segments are deleted
%% when the journal exceeds its disk budget or holds only records older than
%% the retention. Appends are buffered by disk_log and made durable on a
%% timer (history.sync_interval), so a hard crash loses at most that much.
-module(rabbit_access_insight_journal).
-behaviour(gen_server).

-include("rabbit_access_insight.hrl").

-export([start_link/1, append/1, sync/0, read/2, info/0, close/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(MIN_SEGMENT, 1024 * 1024).
-define(MAX_SEGMENT, 64 * 1024 * 1024).
-define(CHUNK, 500).

start_link(Dir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Dir, []).

%% Asynchronous: the caller never waits for the disk.
append([]) -> ok;
append(Records) -> gen_server:cast(?MODULE, {append, Records}).

sync() -> gen_server:call(?MODULE, sync, 30000).

%% Up to Limit records with a sequence number greater than Since.
-spec read(non_neg_integer(), pos_integer() | infinity) -> [tuple()].
read(Since, Limit) -> gen_server:call(?MODULE, {read, Since, Limit}, 60000).

info() -> gen_server:call(?MODULE, info).

close() -> gen_server:call(?MODULE, close, 30000).

%%----------------------------------------------------------------------------

init(Dir) ->
    process_flag(trap_exit, true),
    JDir = filename:join(Dir, "journal"),
    ok = filelib:ensure_dir(filename:join(JDir, "x")),
    Budget = rabbit_access_insight_config:get(history_max_disk),
    SegBytes = max(?MIN_SEGMENT, min(?MAX_SEGMENT, Budget div 16)),
    S0 = #{dir => JDir, budget => Budget, seg_bytes => SegBytes,
           cur => undefined, cur_first => undefined},
    S = case segments(JDir) of
            []   -> S0;
            Segs -> open_segment(lists:last(Segs), S0)
        end,
    schedule_sync(),
    erlang:send_after(3600000, self(), retention),
    {ok, S}.

handle_call(sync, _From, S) ->
    {reply, do_sync(S), S};
handle_call({read, Since, Limit}, _From, S) ->
    _ = do_sync(S),
    {reply, do_read(Since, Limit, S), S};
handle_call(info, _From, S = #{dir := Dir}) ->
    Segs = segments(Dir),
    Bytes = lists:sum([filelib:file_size(seg_path(Dir, F)) || F <- Segs]),
    {reply, #{segments => length(Segs), bytes => Bytes,
              oldest_seq => case Segs of [] -> undefined; [F | _] -> F end}, S};
handle_call(close, _From, S) ->
    {reply, ok, close_current(S)}.

handle_cast({append, Records = [?REC(First, _, _, _) | _]}, S0) ->
    S1 = case S0 of
             #{cur := undefined} -> open_segment(First, S0);
             _ -> S0
         end,
    #{cur := Log} = S1,
    ok = disk_log:log_terms(Log, Records),
    {noreply, maybe_rotate(Records, S1)}.

handle_info(sync, S) ->
    _ = do_sync(S),
    schedule_sync(),
    {noreply, S};
handle_info(retention, S) ->
    erlang:send_after(3600000, self(), retention),
    {noreply, retention(S)};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    _ = close_current(S),
    ok.

%%----------------------------------------------------------------------------

schedule_sync() ->
    erlang:send_after(rabbit_access_insight_config:get(history_sync_interval), self(), sync).

do_sync(#{cur := undefined}) -> ok;
do_sync(#{cur := Log}) -> disk_log:sync(Log).

segments(Dir) ->
    lists:sort([list_to_integer(filename:basename(F, ".log"))
                || F <- filelib:wildcard("*.log", Dir),
                   lists:all(fun(C) -> C >= $0 andalso C =< $9 end, filename:basename(F, ".log"))]).

seg_path(Dir, First) ->
    filename:join(Dir, io_lib:format("~20..0w.log", [First])).

open_segment(First, S = #{dir := Dir}) ->
    S1 = close_current(S),
    Name = {?MODULE, First},
    {ok, Log} = case disk_log:open([{name, Name}, {file, seg_path(Dir, First)},
                                    {type, halt}, {format, internal}, {repair, true}]) of
                    {ok, L} -> {ok, L};
                    {repaired, L, _, _} -> {ok, L}
                end,
    S1#{cur => Log, cur_first => First}.

close_current(S = #{cur := undefined}) -> S;
close_current(S = #{cur := Log}) ->
    _ = disk_log:sync(Log),
    _ = disk_log:close(Log),
    S#{cur => undefined, cur_first => undefined}.

maybe_rotate(Records, S = #{dir := Dir, cur_first := First, seg_bytes := SegBytes}) ->
    case filelib:file_size(seg_path(Dir, First)) >= SegBytes of
        false -> S;
        true ->
            ?REC(Last, _, _, _) = lists:last(Records),
            retention(open_segment(Last + 1, S))
    end.

%% Delete the oldest closed segments while the journal is over budget or
%% they hold only records older than the retention.
retention(S = #{dir := Dir, budget := Budget, cur_first := Cur}) ->
    Days = rabbit_access_insight_config:get(history_log_days),
    Cutoff = rabbit_access_insight_util:now_ms() - Days * 86400000,
    Segs = [F || F <- segments(Dir), F =/= Cur],
    Sizes = [{F, filelib:file_size(seg_path(Dir, F))} || F <- Segs],
    Total = lists:sum([Sz || {_, Sz} <- Sizes]) +
            case Cur of undefined -> 0; _ -> filelib:file_size(seg_path(Dir, Cur)) end,
    drop_old(Sizes, Total, Budget, Cutoff, S).

drop_old([], _Total, _Budget, _Cutoff, S) -> S;
drop_old([{F, Sz} | Rest], Total, Budget, Cutoff, S = #{dir := Dir}) ->
    case Total > Budget orelse last_ts(seg_path(Dir, F)) < Cutoff of
        true  -> _ = file:delete(seg_path(Dir, F)),
                 drop_old(Rest, Total - Sz, Budget, Cutoff, S);
        false -> S
    end.

last_ts(Path) ->
    case fold_file(Path, fun(?REC(_, Ts, _, _), _) -> Ts end, 0) of
        {ok, Ts} -> Ts;
        _ -> 0
    end.

do_read(Since, Limit, S = #{dir := Dir}) ->
    Segs = segments(Dir),
    %% Start from the last segment whose first record is =< Since + 1.
    Start = case [F || F <- Segs, F =< Since + 1] of
                [] -> Segs;
                Earlier -> lists:dropwhile(fun(F) -> F < lists:last(Earlier) end, Segs)
            end,
    read_segments(Start, Since, Limit, S, []).

read_segments([], _Since, _Limit, _S, Acc) -> lists:reverse(Acc);
read_segments(_Segs, _Since, 0, _S, Acc) -> lists:reverse(Acc);
read_segments([F | Rest], Since, Limit, S = #{dir := Dir}, Acc0) ->
    Fun = fun
        (_R, {0, A}) -> {0, A};
        (R = ?REC(Seq, _, _, _), {N, A}) when Seq > Since -> {dec(N), [R | A]};
        (_R, NA) -> NA
    end,
    {ok, {Left, Acc1}} =
        case S of
            %% the segment being appended to is read through its own handle
            #{cur := Log, cur_first := F} when Log =/= undefined ->
                fold_chunks(Log, start, Fun, {Limit, Acc0}, keep_open);
            _ ->
                fold_file(seg_path(Dir, F), Fun, {Limit, Acc0})
        end,
    read_segments(Rest, Since, Left, S, Acc1).

dec(infinity) -> infinity;
dec(N) -> N - 1.

%% Read a closed segment through a private read-only handle.
fold_file(Path, Fun, Acc0) ->
    Name = {?MODULE, read, make_ref()},
    case disk_log:open([{name, Name}, {file, Path}, {type, halt},
                        {format, internal}, {mode, read_only}]) of
        {ok, Log} -> fold_chunks(Log, start, Fun, Acc0, close);
        {repaired, Log, _, _} -> fold_chunks(Log, start, Fun, Acc0, close);
        {error, _} = E -> E
    end.

fold_chunks(Log, Cont, Fun, Acc, Mode) ->
    case disk_log:chunk(Log, Cont, ?CHUNK) of
        eof ->
            done(Log, Mode), {ok, Acc};
        {error, _} = E ->
            done(Log, Mode), E;
        {Cont2, Terms} ->
            fold_chunks(Log, Cont2, Fun, lists:foldl(Fun, Acc, Terms), Mode);
        {Cont2, Terms, _BadBytes} ->
            fold_chunks(Log, Cont2, Fun, lists:foldl(Fun, Acc, Terms), Mode)
    end.

done(Log, close) -> disk_log:close(Log);
done(_Log, keep_open) -> ok.
