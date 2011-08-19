% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_merger).

-export([query_index/3]).

% only needed for couch_view_merger. Those functions should perhaps go into
% a utils module
% open_db/3, dec_counter/1 is also needed by this file
-export([open_db/3, collect_rows/3,
    merge_indexes_no_acc/2, merge_indexes_no_limit/1, handle_skip/1,
    dec_counter/1, get_group_id/2]).

-include("couch_db.hrl").
-include("couch_merger.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1,
    get_nested_json_value/2
]).

-define(LOCAL, <<"local">>).

query_index(Mod, #httpd{user_ctx = UserCtx} = Req, IndexMergeParams) ->
    #index_merge{
       indexes = Indexes, callback = Callback, user_acc = UserAcc,
       conn_timeout = Timeout, extra = Extra
    } = IndexMergeParams,
    {ok, DDoc, IndexName} = get_first_ddoc(Indexes, UserCtx, Timeout),
    IndexArgs = Mod:parse_http_params(Req, DDoc, IndexName, Extra),
    {LessFun, FoldFun, MergeFun, CollectorFun, Extra2} = Mod:make_funs(
        Req, DDoc, IndexName, IndexArgs, IndexMergeParams),
    NumFolders = length(Indexes),
    QueueLessFun = fun
        ({error, _Url, _Reason}, _) ->
            true;
        (_, {error, _Url, _Reason}) ->
            false;
        ({row_count, _}, _) ->
            true;
        (_, {row_count, _}) ->
            false;
        (RowA, RowB) ->
            case LessFun of
            nil ->
                % That's where the actual less fun is. But as bounding box
                % requests don't return a sorted order, we just return true
                true;
             _ ->
                LessFun(RowA, RowB)
            end
    end,
    {ok, Queue} = couch_view_merger_queue:start_link(NumFolders, QueueLessFun),
    Collector = spawn_link(fun() ->
        CollectorFun(NumFolders, Callback, UserAcc)
    end),
    Folders = lists:foldr(
        fun(Index, Acc) ->
            Pid = spawn_link(fun() ->
                index_folder(Mod, Index, IndexMergeParams, UserCtx, IndexArgs,
                    Queue, FoldFun)
                %FoldFun(Index, IndexMergeParams, UserCtx, IndexArgs, Queue)
            end),
            [Pid | Acc]
        end,
        [], Indexes),
    {Skip, Limit} = Mod:get_skip_and_limit(IndexArgs),
    MergeParams = #merge_params{
        index_name = IndexName,
        queue = Queue,
        collector = Collector,
        skip = Skip,
        limit = Limit,
        extra = Extra2
    },
    case MergeFun(MergeParams) of
    {ok, Resp} ->
        ok;
    {stop, Resp} ->
        lists:foreach(
            fun(P) -> catch unlink(P), catch exit(P, kill) end, Folders)
    end,
    catch unlink(Queue),
    catch exit(Queue, kill),
    Resp.


get_first_ddoc([], _UserCtx, _Timeout) ->
    throw({error, <<"A view spec can not consist of merges exclusively.">>});

get_first_ddoc([#simple_view_spec{view_name = <<"_all_docs">>} | _],
               _UserCtx, _Timeout) ->
    {ok, nil, <<"_all_docs">>};

get_first_ddoc([#simple_view_spec{} = Spec | _], UserCtx, Timeout) ->
    #simple_view_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = Id,
        view_name = IndexName
    } = Spec,
    {ok, Db} = case DDocDbName of
    nil ->
        open_db(DbName, UserCtx, Timeout);
    _ when is_binary(DDocDbName) ->
        open_db(DDocDbName, UserCtx, Timeout)
    end,
    {ok, #doc{body = DDoc}} = get_ddoc(Db, Id),
    close_db(Db),
    {ok, DDoc, IndexName};

get_first_ddoc([_MergeSpec | Rest], UserCtx, Timeout) ->
    get_first_ddoc(Rest, UserCtx, Timeout).


open_db(<<"http://", _/binary>> = DbName, _UserCtx, Timeout) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = Timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(<<"https://", _/binary>> = DbName, _UserCtx, Timeout) ->
    HttpDb = #httpdb{
        url = maybe_add_trailing_slash(DbName),
        timeout = Timeout
    },
    {ok, HttpDb#httpdb{ibrowse_options = ibrowse_options(HttpDb)}};
open_db(DbName, UserCtx, _Timeout) ->
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _} ->
        throw({not_found, db_not_found_msg(DbName)});
    Error ->
        throw(Error)
    end.

maybe_add_trailing_slash(Url) when is_binary(Url) ->
    maybe_add_trailing_slash(?b2l(Url));
maybe_add_trailing_slash(Url) ->
    case lists:last(Url) of
    $/ ->
        Url;
    _ ->
        Url ++ "/"
    end.

close_db(#httpdb{}) ->
    ok;
close_db(Db) ->
    couch_db:close(Db).

get_ddoc(#httpdb{url = BaseUrl, headers = Headers} = HttpDb, Id) ->
    Url = BaseUrl ++ ?b2l(Id),
    case ibrowse:send_req(
        Url, Headers, get, [], HttpDb#httpdb.ibrowse_options) of
    {ok, "200", _RespHeaders, Body} ->
        {ok, couch_doc:from_json_obj(?JSON_DECODE(Body))};
    {ok, _Code, _RespHeaders, Body} ->
        {Props} = ?JSON_DECODE(Body),
        case {get_value(<<"error">>, Props), get_value(<<"reason">>, Props)} of
        {not_found, _} ->
            throw({not_found, ddoc_not_found_msg(HttpDb, Id)});
        Error ->
            Msg = io_lib:format("Error getting design document `~s` from "
                "database `~s`: ~s", [Id, db_uri(HttpDb), Error]),
            throw({error, iolist_to_binary(Msg)})
        end;
    {error, Error} ->
        Msg = io_lib:format("Error getting design document `~s` from database "
            "`~s`: ~s", [Id, db_uri(HttpDb), Error]),
        throw({error, iolist_to_binary(Msg)})
    end;
get_ddoc(Db, Id) ->
    case couch_db:open_doc(Db, Id, [ejson_body]) of
    {ok, _} = Ok ->
        Ok;
    {not_found, _} ->
        throw({not_found, ddoc_not_found_msg(Db, Id)})
    end.

% Returns the group ID of the indexer group that contains the Design Document
% In Couchbase the Design Document is stored in a so-called master database.
% This is Couchbase specific
get_group_id(nil, DDocId) ->
    DDocId;
get_group_id(DDocDbName, DDocId) when is_binary(DDocDbName) ->
    DDocDb = case couch_db:open_int(DDocDbName, []) of
    {ok, DDocDb1} ->
        DDocDb1;
    {not_found, _} ->
        throw(ddoc_db_not_found)
    end,
    {DDocDb, DDocId}.

db_uri(#httpdb{url = Url}) ->
    db_uri(Url);
db_uri(#db{name = Name}) ->
    Name;
db_uri(Url) when is_binary(Url) ->
    ?l2b(couch_util:url_strip_password(Url)).


db_not_found_msg(DbName) ->
    iolist_to_binary(io_lib:format("Database `~s` doesn't exist.", [db_uri(DbName)])).

ddoc_not_found_msg(DbName, DDocId) ->
    Msg = io_lib:format(
        "Design document `~s` missing in database `~s`.", [DDocId, db_uri(DbName)]),
    iolist_to_binary(Msg).


ibrowse_options(#httpdb{timeout = T, url = Url}) ->
    [{inactivity_timeout, T}, {connect_timeout, infinity},
        {response_format, binary}, {socket_options, [{keepalive, true}]}] ++
    case Url of
    "https://" ++ _ ->
        % TODO: add SSL options like verify and cacertfile
        [{is_ssl, true}];
    _ ->
        []
    end.


% PreprocessFun is called on every row (which comes from the fold function
% of the underlying data structure) before it gets passed into the Callback
% function
collect_rows(PreprocessFun, Callback, UserAcc) ->
    receive
    {{error, _DbUrl, _Reason} = Error, From} ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            From ! {stop, Resp, self()};
        {ok, UserAcc2} ->
            From ! {continue, self()},
            collect_rows(PreprocessFun, Callback, UserAcc2)
        end;
    {row, Row} ->
        RowEJson = PreprocessFun(Row),
        {ok, UserAcc2} = Callback({row, RowEJson}, UserAcc),
        collect_rows(PreprocessFun, Callback, UserAcc2);
    {stop, From} ->
        {ok, UserAcc2} = Callback(stop, UserAcc),
        From ! {UserAcc2, self()}
    end.

% When no limit is specified the merging is easy
merge_indexes_no_limit(#merge_params{collector = Col}) ->
    Col ! {stop, self()},
    receive
    {Resp, Col} ->
        {stop, Resp}
    end.

% Simple case when there are no (or we don't care about) accumulated rows
% MinRowFun is a function that it called if the
% couch_view_merger_queue returns a row that is neither an error, nor a count.
merge_indexes_no_acc(Params, MinRowFun) ->
    #merge_params{
        queue = Queue, collector = Col
    } = Params,
    case couch_view_merger_queue:pop(Queue) of
    closed ->
        Col ! {stop, self()},
        receive
        {Resp, Col} ->
            {ok, Resp}
        end;
    {ok, {error, _Url, _Reason} = Error} ->
        Col ! {Error, self()},
        ok = couch_view_merger_queue:flush(Queue),
        receive
        {continue, Col} ->
            merge_indexes_no_acc(Params, MinRowFun);
        {stop, Resp, Col} ->
            {stop, Resp}
        end;
    {ok, {row_count, _} = RowCount} ->
        Col ! RowCount,
        ok = couch_view_merger_queue:flush(Queue),
        merge_indexes_no_acc(Params, MinRowFun);
    {ok, MinRow} ->
        Params2 = MinRowFun(Params, MinRow),
        {params, Params2}
    end.

handle_skip(Params) ->
    #merge_params{
        limit = Limit, skip = Skip, collector = Col,
        row_acc = [RowToSend | Rest]
    } = Params,
    case Skip > 0 of
    true ->
        Limit2 = Limit;
    false ->
        Col ! {row, RowToSend},
        Limit2 = dec_counter(Limit)
    end,
    Params#merge_params{
        skip = dec_counter(Skip), limit = Limit2, row_acc = Rest
    }.

dec_counter(0) -> 0;
dec_counter(N) -> N - 1.


index_folder(Mod, #simple_view_spec{database = <<"http://", _/binary>>} =
        IndexSpec, MergeParams, _UserCtx, IndexArgs, Queue, _FoldFun) ->
    http_index_folder(Mod, IndexSpec, MergeParams, IndexArgs, Queue);

index_folder(Mod, #simple_view_spec{database = <<"https://", _/binary>>} =
        IndexSpec, MergeParams, _UserCtx, IndexArgs, Queue, _FoldFun) ->
    http_index_folder(Mod, IndexSpec, MergeParams, IndexArgs, Queue);

index_folder(Mod, #merged_view_spec{} = IndexSpec,
        MergeParams, _UserCtx, IndexArgs, Queue, _FoldFun) ->
    http_index_folder(Mod, IndexSpec, MergeParams, IndexArgs, Queue);

index_folder(_Mod, IndexSpec, MergeParams, UserCtx, IndexArgs, Queue,
        FoldFun) ->
    #simple_view_spec{
        database = DbName, ddoc_database = DDocDbName, ddoc_id = DDocId
    } = IndexSpec,
    case couch_db:open(DbName, [{user_ctx, UserCtx}]) of
    {ok, Db} ->
        try
            FoldFun(Db, IndexSpec, MergeParams, IndexArgs, Queue)
        catch
        {not_found, Reason} when Reason =:= missing; Reason =:= deleted ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DbName, DDocId)});
        ddoc_db_not_found ->
            ok = couch_view_merger_queue:queue(
                Queue, {error, ?LOCAL, ddoc_not_found_msg(DDocDbName, DDocId)});
        _Tag:Error ->
            couch_view_merger_queue:queue(Queue, parse_error(Error))
        after
            ok = couch_view_merger_queue:done(Queue),
            couch_db:close(Db)
        end;
    {not_found, _} ->
        ok = couch_view_merger_queue:queue(
            Queue, {error, ?LOCAL, db_not_found_msg(DbName)}),
        ok = couch_view_merger_queue:done(Queue)
    end.

% `invalid_value` only happens on reduces
parse_error({invalid_value, Reason}) ->
    {error, ?LOCAL, to_binary(Reason)};
parse_error(Error) ->
    {error, ?LOCAL, to_binary(Error)}.

% Fold function for remote indexes
http_index_folder(Mod, IndexSpec, MergeParams, IndexArgs, Queue) ->
    EventFun = Mod:make_event_fun(IndexArgs, Queue),
    {Url, Method, Headers, Body, Options} = Mod:http_index_folder_req_details(
        IndexSpec, MergeParams, IndexArgs),
    {ok, Conn} = ibrowse:spawn_link_worker_process(Url),
    {ibrowse_req_id, ReqId} = ibrowse:send_req_direct(
        Conn, Url, Headers, Method, Body,
        [{stream_to, {self(), once}} | Options]),
    receive
    {ibrowse_async_headers, ReqId, "200", _RespHeaders} ->
        ibrowse:stream_next(ReqId),
        DataFun = fun() -> stream_data(ReqId) end,
        try
            json_stream_parse:events(DataFun, EventFun)
        catch throw:{error, Error} ->
            ok = couch_view_merger_queue:queue(Queue, {error, Url, Error})
        after
            stop_conn(Conn),
            ok = couch_view_merger_queue:done(Queue)
        end;
    {ibrowse_async_headers, ReqId, Code, _RespHeaders} ->
        Error = try
            stream_all(ReqId, [])
        catch throw:{error, _Error} ->
            <<"Error code ", (?l2b(Code))/binary>>
        end,
        case (catch ?JSON_DECODE(Error)) of
        {Props} when is_list(Props) ->
            case {get_value(<<"error">>, Props), get_value(<<"reason">>, Props)} of
            {<<"not_found">>, Reason} when
                    Reason =/= <<"missing">>, Reason =/= <<"deleted">> ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, Reason});
            {<<"not_found">>, _} ->
                ok = couch_view_merger_queue:queue(Queue, {error, Url, <<"not_found">>});
            JsonError ->
                ok = couch_view_merger_queue:queue(
                    Queue, {error, Url, to_binary(JsonError)})
            end;
        _ ->
            ok = couch_view_merger_queue:queue(Queue, {error, Url, to_binary(Error)})
        end,
        ok = couch_view_merger_queue:done(Queue),
        stop_conn(Conn);
    {ibrowse_async_response, ReqId, {error, Error}} ->
        stop_conn(Conn),
        ok = couch_view_merger_queue:queue(Queue, {error, Url, Error}),
        ok = couch_view_merger_queue:done(Queue)
    end.


stop_conn(Conn) ->
    unlink(Conn),
    receive {'EXIT', Conn, _} -> ok after 0 -> ok end,
    catch ibrowse:stop_worker_process(Conn).


stream_data(ReqId) ->
    receive
    {ibrowse_async_response, ReqId, {error, _} = Error} ->
        throw(Error);
    {ibrowse_async_response, ReqId, <<>>} ->
        ibrowse:stream_next(ReqId),
        stream_data(ReqId);
    {ibrowse_async_response, ReqId, Data} ->
        ibrowse:stream_next(ReqId),
        {Data, fun() -> stream_data(ReqId) end};
    {ibrowse_async_response_end, ReqId} ->
        {<<>>, fun() -> throw({error, <<"more view data expected">>}) end}
    end.


stream_all(ReqId, Acc) ->
    case stream_data(ReqId) of
    {<<>>, _} ->
        iolist_to_binary(lists:reverse(Acc));
    {Data, _} ->
        stream_all(ReqId, [Data | Acc])
    end.
