%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 martinx
%% SPDX-License-Identifier: MPL-2.0
%%
%% Files next to the journal: the epoch, snapshots, and the clean-shutdown
%% marker.
%%
%%   epoch     random id created with the history directory; together with
%%             the node name it identifies this node's contribution
%%   snapshot  all aggregates and open sessions at a sequence number; the
%%             journal records after it are replayed on start
%%   clean     written on an orderly stop, removed on start
-module(rabbit_access_insight_store).

-export([epoch/1, save_snapshot/2, load_snapshot/1, mark_clean/2, take_clean/1]).

-define(FORMAT, 1).

%% {Epoch, New}: New is true when the directory had no epoch, in which case
%% any journal or snapshot left there belongs to no known epoch and is removed.
-spec epoch(file:filename()) -> {binary(), boolean()}.
epoch(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    File = filename:join(Dir, "epoch"),
    case file:read_file(File) of
        {ok, <<E:32/binary, _/binary>>} ->
            {E, false};
        _ ->
            _ = file:delete(filename:join(Dir, "snapshot")),
            [file:delete(F) || F <- filelib:wildcard(filename:join([Dir, "journal", "*.log"]))],
            E = << <<(hex(N))>> || <<N:4>> <= crypto:strong_rand_bytes(16) >>,
            ok = write_atomic(File, E),
            {E, true}
    end.

-spec save_snapshot(file:filename(), map()) -> ok.
save_snapshot(Dir, Snapshot) ->
    write_atomic(filename:join(Dir, "snapshot"),
                 term_to_binary(Snapshot#{format => ?FORMAT}, [compressed])).

-spec load_snapshot(file:filename()) -> {ok, map()} | none.
load_snapshot(Dir) ->
    case file:read_file(filename:join(Dir, "snapshot")) of
        {ok, Bin} ->
            try binary_to_term(Bin) of
                #{format := ?FORMAT} = S -> {ok, S};
                _ -> none
            catch _:_ -> none
            end;
        _ -> none
    end.

mark_clean(Dir, Ts) ->
    write_atomic(filename:join(Dir, "clean"), integer_to_binary(Ts)).

%% {clean, StoppedAtMs} if the last stop was orderly, else unclean.
take_clean(Dir) ->
    File = filename:join(Dir, "clean"),
    R = case file:read_file(File) of
            {ok, B} -> try {clean, binary_to_integer(B)} catch _:_ -> unclean end;
            _ -> unclean
        end,
    _ = file:delete(File),
    R.

hex(N) when N < 10 -> $0 + N;
hex(N) -> $a + N - 10.

%% Write to a temporary file, fsync, then rename over the target, so a crash
%% leaves either the old or the new content, never a torn file.
write_atomic(File, Data) ->
    Tmp = File ++ ".tmp",
    {ok, Fd} = file:open(Tmp, [write, raw, binary]),
    ok = file:write(Fd, Data),
    ok = file:sync(Fd),
    ok = file:close(Fd),
    ok = file:rename(Tmp, File).
