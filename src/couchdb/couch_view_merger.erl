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

-module(couch_view_merger).

% export callbacks
-export([parse_http_params/4, make_funs/5, get_skip_and_limit/1,
    http_index_folder_req_details/3, make_event_fun/2]).

%-export([query_view/2]).


% Only needed for GeoCouch
-export([http_view_fold_queue_error/2, void_event/1]).
%-export([collector_loop/4, dec_counter/1, get_first_ddoc/3, http_view_fold/3,
%    make_map_fold_fun/4, open_db/3, stop_conn/1, stream_all/2, stream_data/1]).

-include("couch_db.hrl").
-include("couch_merger.hrl").
-include("couch_view_merger.hrl").

-define(LOCAL, <<"local">>).

-import(couch_util, [
    get_value/2,
    get_value/3,
    to_binary/1,
    get_nested_json_value/2
]).


% callback!
parse_http_params(Req, DDoc, ViewName, #view_merge{keys = Keys}) ->
    % view type =~ query type
    {_Collation, ViewType0, _ViewLang} = view_details(DDoc, ViewName),
    ViewType = case {ViewType0, couch_httpd:qs_value(Req, "reduce", "true")} of
    {reduce, "false"} ->
       red_map;
    _ ->
       ViewType0
    end,
    couch_httpd_view:parse_view_params(Req, Keys, ViewType).

% callback!
make_funs(Req, DDoc, ViewName, ViewArgs, IndexMergeParams) ->
    #index_merge{
       extra = Extra
    } = IndexMergeParams,
    #view_merge{
       rereduce_fun = InRedFun, rereduce_fun_lang = InRedFunLang
    } = Extra,
    {Collation, ViewType0, ViewLang} = view_details(DDoc, ViewName),
    ViewType = case {ViewType0, couch_httpd:qs_value(Req, "reduce", "true")} of
    {reduce, "false"} ->
       red_map;
    _ ->
       ViewType0
    end,
    {RedFun, RedFunLang} = case {ViewType, InRedFun} of
    {reduce, nil} ->
        {reduce_function(DDoc, ViewName), ViewLang};
    {reduce, _} when is_binary(InRedFun) ->
        {InRedFun, InRedFunLang};
    _ ->
        {nil, nil}
    end,
    LessFun = view_less_fun(Collation, ViewArgs#view_query_args.direction,
        ViewType),
    {FoldFun, MergeFun} = case ViewType of
    reduce ->
        {fun reduce_view_folder/5, fun merge_reduce_views/1};
    _ when ViewType =:= map; ViewType =:= red_map ->
        {fun map_view_folder/5, fun merge_map_views/1}
    end,
    CollectorFun = case ViewType of
    reduce ->
        fun (_NumFolders, Callback2, UserAcc2) ->
            {ok, UserAcc3} = Callback2(start, UserAcc2),
            couch_merger:collect_rows(
                fun view_row_obj_reduce/1, Callback2, UserAcc3)
        end;
     % red_map|map
     _ ->
        fun (NumFolders, Callback2, UserAcc2) ->
            collect_row_count(
                NumFolders, 0, fun view_row_obj_map/1, Callback2, UserAcc2)
        end
    end,
    Extra2 = #view_merge{
        rereduce_fun = RedFun,
        rereduce_fun_lang = RedFunLang
    },
    {LessFun, FoldFun, MergeFun, CollectorFun, Extra2}.

% callback!
get_skip_and_limit(#view_query_args{skip=Skip, limit=Limit}) ->
    {Skip, Limit}.

% callback!
make_event_fun(ViewArgs, Queue) ->
    fun(Ev) ->
        http_view_fold(Ev, ViewArgs#view_query_args.view_type, Queue)
    end.

% callback!
http_index_folder_req_details(#merged_view_spec{
        url = MergeUrl0, ejson_spec = {EJson}}, MergeParams, ViewArgs) ->
    #index_merge{
        conn_timeout = Timeout,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        couch_merger:open_db(MergeUrl0, nil, Timeout),
    MergeUrl = Url ++ view_qs(ViewArgs),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],
    Body = case Keys of
    nil ->
        {EJson};
    _ ->
        {[{<<"keys">>, Keys} | EJson]}
    end,
    put(from_url, Url),
    {MergeUrl, post, Headers, ?JSON_ENCODE(Body), Options};

http_index_folder_req_details(#simple_view_spec{
        database = DbUrl, ddoc_id = DDocId, view_name = ViewName},
        MergeParams, ViewArgs) ->
    #index_merge{
        conn_timeout = Timeout,
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, #httpdb{url = Url, ibrowse_options = Options} = Db} =
        couch_merger:open_db(DbUrl, nil, Timeout),
    ViewUrl = Url ++ case ViewName of
    <<"_all_docs">> ->
        "_all_docs";
    _ ->
        ?b2l(DDocId) ++ "/_view/" ++ ?b2l(ViewName)
    end ++ view_qs(ViewArgs),
    Headers = [{"Content-Type", "application/json"} | Db#httpdb.headers],
    put(from_url, DbUrl),
    case Keys of
    nil ->
        {ViewUrl, get, [], [], Options};
    _ ->
        {ViewUrl, post, Headers, ?JSON_ENCODE({[{<<"keys">>, Keys}]}), Options}
    end.


view_details(nil, <<"_all_docs">>) ->
    {<<"raw">>, map, nil};

view_details({Props} = DDoc, ViewName) ->
    {ViewDef} = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    {ViewOptions} = get_value(<<"options">>, ViewDef, {[]}),
    Collation = get_value(<<"collation">>, ViewOptions, <<"default">>),
    ViewType = case get_value(<<"reduce">>, ViewDef) of
    undefined ->
        map;
    RedFun when is_binary(RedFun) ->
        reduce
    end,
    Lang = get_value(<<"language">>, Props, <<"javascript">>),
    {Collation, ViewType, Lang}.


reduce_function(DDoc, ViewName) ->
    {ViewDef} = get_nested_json_value(DDoc, [<<"views">>, ViewName]),
    get_value(<<"reduce">>, ViewDef).


view_less_fun(Collation, Dir, ViewType) ->
    LessFun = case Collation of
    <<"default">> ->
        case ViewType of
        _ when ViewType =:= map; ViewType =:= red_map ->
            fun(RowA, RowB) ->
                couch_view:less_json_ids(element(1, RowA), element(1, RowB))
            end;
        reduce ->
            fun({KeyA, _}, {KeyB, _}) -> couch_view:less_json(KeyA, KeyB) end
        end;
    <<"raw">> ->
        fun(A, B) -> A < B end
    end,
    case Dir of
    fwd ->
        LessFun;
    rev ->
        fun(A, B) -> not LessFun(A, B) end
    end.

collect_row_count(RecvCount, AccCount, PreprocessFun, Callback, UserAcc) ->
    receive
    {{error, _DbUrl, _Reason} = Error, From} ->
        case Callback(Error, UserAcc) of
        {stop, Resp} ->
            From ! {stop, Resp, self()};
        {ok, UserAcc2} ->
            From ! {continue, self()},
            case RecvCount > 1 of
            false ->
                {ok, UserAcc3} = Callback({start, AccCount}, UserAcc2),
                couch_merger:collect_rows(PreprocessFun, Callback, UserAcc3);
            true ->
                collect_row_count(
                    RecvCount - 1, AccCount, PreprocessFun, Callback, UserAcc2)
            end
        end;
    {row_count, Count} ->
        AccCount2 = AccCount + Count,
        case RecvCount > 1 of
        false ->
            % TODO: what about offset and update_seq?
            % TODO: maybe add etag like for regular views? How to
            %       compute them?
            {ok, UserAcc2} = Callback({start, AccCount2}, UserAcc),
            couch_merger:collect_rows(PreprocessFun, Callback, UserAcc2);
        true ->
            collect_row_count(
                RecvCount - 1, AccCount2, PreprocessFun, Callback, UserAcc)
        end
    end.

view_row_obj_map({{Key, error}, Value}) ->
    {[{key, Key}, {error, Value}]};

view_row_obj_map({{Key, DocId}, Value}) ->
    {[{id, DocId}, {key, Key}, {value, Value}]};

view_row_obj_map({{Key, DocId}, Value, Doc}) ->
    {[{id, DocId}, {key, Key}, {value, Value}, Doc]}.

view_row_obj_reduce({Key, Value}) ->
    {[{key, Key}, {value, Value}]}.


merge_map_views(#merge_params{limit = 0} = Params) ->
    couch_merger:merge_indexes_no_limit(Params);

merge_map_views(#merge_params{row_acc = []} = Params) ->
    case couch_merger:merge_indexes_no_acc(Params, fun merge_map_min_row/2) of
    {params, Params2} ->
        merge_map_views(Params2);
    Else ->
        Else
    end;

merge_map_views(Params) ->
    Params2 = couch_merger:handle_skip(Params),
    merge_map_views(Params2).


% A new Params record is returned
merge_map_min_row(Params, MinRow) ->
    #merge_params{
        queue = Queue, index_name = ViewName
    } = Params,
    {RowToSend, RestToSend} = handle_duplicates(ViewName, MinRow, Queue),
    ok = couch_view_merger_queue:flush(Queue),
    couch_merger:handle_skip(
        Params#merge_params{row_acc=[RowToSend|RestToSend]}).



handle_duplicates(<<"_all_docs">>, MinRow, Queue) ->
    handle_all_docs_row(MinRow, Queue);

handle_duplicates(_ViewName, MinRow, Queue) ->
    handle_duplicates_allowed(MinRow, Queue).


handle_all_docs_row(MinRow, Queue) ->
    {Key0, Id0} = element(1, MinRow),
    % Group rows by similar keys, split error "not_found" from normal ones. If all
    % are "not_found" rows, squash them into one. If there are "not_found" ones
    % and others with a value, discard the "not_found" ones.
    {ValueRows, ErrorRows} = case Id0 of
    error ->
        pop_similar_rows(Key0, Queue, [], [MinRow]);
    _ when is_binary(Id0) ->
        pop_similar_rows(Key0, Queue, [MinRow], [])
    end,
    case {ValueRows, ErrorRows} of
    {[], [ErrRow | _]} ->
        {ErrRow, []};
    {[ValRow], []} ->
        {ValRow, []};
    {[FirstVal | RestVal], _} ->
        {FirstVal, RestVal}
    end.

handle_duplicates_allowed(MinRow, _Queue) ->
    {MinRow, []}.

pop_similar_rows(Key0, Queue, Acc, AccError) ->
    case couch_view_merger_queue:peek(Queue) of
    empty ->
        {Acc, AccError};
    {ok, Row} ->
        {Key, DocId} = element(1, Row),
        case Key =:= Key0 of
        false ->
            {Acc, AccError};
        true ->
            {ok, Row} = couch_view_merger_queue:pop_next(Queue),
            case DocId of
            error ->
                pop_similar_rows(Key0, Queue, Acc, [Row | AccError]);
            _ ->
                pop_similar_rows(Key0, Queue, [Row | Acc], AccError)
            end
        end
    end.


merge_reduce_views(#merge_params{limit = 0} = Params) ->
    couch_merger:merge_indexes_no_limit(Params);

merge_reduce_views(Params) ->
    case couch_merger:merge_indexes_no_acc(Params, fun merge_reduce_min_row/2) of
    {params, Params2} ->
        merge_reduce_views(Params2);
    Else ->
        Else
    end.

merge_reduce_min_row(Params, MinRow) ->
    #merge_params{
        queue = Queue, limit = Limit, skip = Skip, collector = Col
    } = Params,
    RowGroup = group_keys_for_rereduce(Queue, [MinRow]),
    ok = couch_view_merger_queue:flush(Queue),
    Row = case RowGroup of
    [R] ->
        {row, R};
    [{K, _}, _ | _] ->
        try
            RedVal = rereduce(RowGroup, Params),
            {row, {K, RedVal}}
        catch
        _Tag:Error ->
            on_rereduce_error(Col, Error)
        end
    end,
    case Row of
    {stop, _Resp} = Stop ->
        Stop;
    _ ->
        case Skip > 0 of
        true ->
            Limit2 = Limit;
        false ->
            case Row of
            {row, _} ->
                Col ! Row;
            _ ->
                ok
            end,
            Limit2 = couch_merger:dec_counter(Limit)
        end,
        Params#merge_params{
            skip = couch_merger:dec_counter(Skip), limit = Limit2
        }
    end.


on_rereduce_error(Col, Error) ->
    Col ! {reduce_error(Error), self()},
    receive
    {continue, Col} ->
        ok;
    {stop, Resp, Col} ->
        {stop, Resp}
    end.


reduce_error({invalid_value, Reason}) ->
    {error, ?LOCAL, to_binary(Reason)};
reduce_error(Error) ->
    {error, ?LOCAL, to_binary(Error)}.


group_keys_for_rereduce(Queue, [{K, _} | _] = Acc) ->
    case couch_view_merger_queue:peek(Queue) of
    empty ->
        Acc;
    {ok, {K, _} = Row} ->
        {ok, Row} = couch_view_merger_queue:pop_next(Queue),
        group_keys_for_rereduce(Queue, [Row | Acc]);
    {ok, _} ->
        Acc
    end.


rereduce(Rows, #merge_params{extra = Extra}) ->
    #view_merge{
        rereduce_fun = RedFun,
        rereduce_fun_lang = Lang
    } = Extra,
    Reds = [[Val] || {_Key, Val} <- Rows],
    {ok, [Value]} = couch_query_servers:rereduce(Lang, [RedFun], Reds),
    Value.


map_view_folder(Db, #simple_view_spec{view_name = <<"_all_docs">>},
        MergeParams, ViewArgs, Queue) ->
    #index_merge{
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    {ok, Info} = couch_db:get_db_info(Db),
    ok = couch_view_merger_queue:queue(
        Queue, {row_count, get_value(doc_count, Info)}),
    % TODO: add support for ?update_seq=true and offset
    fold_local_all_docs(Keys, Db, Queue, ViewArgs);

map_view_folder(Db, ViewSpec, MergeParams, ViewArgs, Queue) ->
    #simple_view_spec{
        ddoc_database = DDocDbName, ddoc_id = DDocId, view_name = ViewName
    } = ViewSpec,
    #index_merge{
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    #view_query_args{
        stale = Stale,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    FoldlFun = make_map_fold_fun(IncludeDocs, Conflicts, Db, Queue),
    {DDocDb, View} = get_map_view(Db, DDocDbName, DDocId, ViewName, Stale),
    {ok, RowCount} = couch_view:get_row_count(View),
    ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
    case Keys of
    nil ->
        FoldOpts = couch_httpd_view:make_key_options(ViewArgs),
        {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts);
    _ when is_list(Keys) ->
        lists:foreach(
            fun(K) ->
                FoldOpts = couch_httpd_view:make_key_options(
                    ViewArgs#view_query_args{start_key = K, end_key = K}),
                {ok, _, _} = couch_view:fold(View, FoldlFun, [], FoldOpts)
            end,
            Keys)
    end,
    catch couch_db:close(DDocDb).


fold_local_all_docs(nil, Db, Queue, ViewArgs) ->
    #view_query_args{
        start_key = StartKey,
        start_docid = StartDocId,
        end_key = EndKey,
        end_docid = EndDocId,
        direction = Dir,
        inclusive_end = InclusiveEnd,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    StartId = if is_binary(StartKey) -> StartKey;
        true -> StartDocId
    end,
    EndId = if is_binary(EndKey) -> EndKey;
        true -> EndDocId
    end,
    FoldOptions = [
        {start_key, StartId}, {dir, Dir},
        {if InclusiveEnd -> end_key; true -> end_key_gt end, EndId}
    ],
    FoldFun = fun(FullDocInfo, _Offset, Acc) ->
        DocInfo = couch_doc:to_doc_info(FullDocInfo),
        #doc_info{revs = [#rev_info{deleted = Deleted} | _]} = DocInfo,
        case Deleted of
        true ->
            ok;
        false ->
            Row = all_docs_row(DocInfo, Db, IncludeDocs, Conflicts),
            ok = couch_view_merger_queue:queue(Queue, Row)
        end,
        {ok, Acc}
    end,
    {ok, _LastOffset, _} = couch_db:enum_docs(Db, FoldFun, [], FoldOptions);

fold_local_all_docs(Keys, Db, Queue, ViewArgs) ->
    #view_query_args{
        direction = Dir,
        include_docs = IncludeDocs,
        conflicts = Conflicts
    } = ViewArgs,
    FoldFun = case Dir of
    fwd ->
        fun lists:foldl/3;
    rev ->
        fun lists:foldr/3
    end,
    FoldFun(
        fun(Key, _Acc) ->
            Row = case (catch couch_db:get_doc_info(Db, Key)) of
            {ok, #doc_info{} = DocInfo} ->
                all_docs_row(DocInfo, Db, IncludeDocs, Conflicts);
            not_found ->
                {{Key, error}, not_found}
            end,
            ok = couch_view_merger_queue:queue(Queue, Row)
        end, [], Keys).


all_docs_row(DocInfo, Db, IncludeDoc, Conflicts) ->
    #doc_info{id = Id, revs = [RevInfo | _]} = DocInfo,
    #rev_info{rev = Rev, deleted = Del} = RevInfo,
    Value = {[{<<"rev">>, couch_doc:rev_to_str(Rev)}] ++ case Del of
    true ->
        [{<<"deleted">>, true}];
    false ->
        []
    end},
    case IncludeDoc of
    true ->
        case Del of
        true ->
            DocVal = {<<"doc">>, null};
        false ->
            DocOptions = if Conflicts -> [conflicts]; true -> [] end,
            [DocVal] = couch_httpd_view:doc_member(Db, DocInfo, DocOptions),
            DocVal
        end,
        {{Id, Id}, Value, DocVal};
    false ->
        {{Id, Id}, Value}
    end.



http_view_fold(object_start, map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, red_map, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end;
http_view_fold(object_start, reduce, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rc_1({key, <<"total_rows">>}, Queue) ->
    fun(Ev) -> http_view_fold_rc_2(Ev, Queue) end;
http_view_fold_rc_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rc_1(Ev, Queue) end.

http_view_fold_rc_2(RowCount, Queue) when is_number(RowCount) ->
    ok = couch_view_merger_queue:queue(Queue, {row_count, RowCount}),
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_1({key, <<"rows">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_rows_2(Ev, Queue) end end;
http_view_fold_rows_1(_Ev, Queue) ->
    fun(Ev) -> http_view_fold_rows_1(Ev, Queue) end.

http_view_fold_rows_2(array_end, Queue) ->
    fun(Ev) -> http_view_fold_errors_1(Ev, Queue) end;
http_view_fold_rows_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Row) ->
                http_view_fold_queue_row(Row, Queue),
                fun(Ev2) -> http_view_fold_rows_2(Ev2, Queue) end
            end)
    end.

http_view_fold_errors_1({key, <<"errors">>}, Queue) ->
    fun(array_start) -> fun(Ev) -> http_view_fold_errors_2(Ev, Queue) end end;
http_view_fold_errors_1(_Ev, _Queue) ->
    fun void_event/1.

http_view_fold_errors_2(array_end, _Queue) ->
    fun void_event/1;
http_view_fold_errors_2(object_start, Queue) ->
    fun(Ev) ->
        json_stream_parse:collect_object(
            Ev,
            fun(Error) ->
                http_view_fold_queue_error(Error, Queue),
                fun(Ev2) -> http_view_fold_errors_2(Ev2, Queue) end
            end)
    end.


http_view_fold_queue_error({Props}, Queue) ->
    From0 = get_value(<<"from">>, Props, ?LOCAL),
    From = case From0 of
        ?LOCAL ->
        get(from_url);
    _ ->
        From0
    end,
    Reason = get_value(<<"reason">>, Props, null),
    ok = couch_view_merger_queue:queue(Queue, {error, From, Reason}).


http_view_fold_queue_row({Props}, Queue) ->
    Key = get_value(<<"key">>, Props, null),
    Id = get_value(<<"id">>, Props, nil),
    Val = get_value(<<"value">>, Props),
    Row = case get_value(<<"error">>, Props, nil) of
    nil ->
        case Id of
        nil ->
            % reduce row
            {Key, Val};
        _ ->
            % map row
            case get_value(<<"doc">>, Props, nil) of
            nil ->
                {{Key, Id}, Val};
            Doc ->
                {{Key, Id}, Val, {doc, Doc}}
            end
        end;
    Error ->
        % error in a map row
        {{Key, error}, Error}
    end,
    ok = couch_view_merger_queue:queue(Queue, Row).

void_event(_Ev) ->
    fun void_event/1.


reduce_view_folder(Db, ViewSpec, MergeParams, ViewArgs, Queue) ->
    #simple_view_spec{
        ddoc_database = DDocDbName, ddoc_id = DDocId, view_name = ViewName
    } = ViewSpec,
    #index_merge{
        extra = #view_merge{
            keys = Keys
        }
    } = MergeParams,
    #view_query_args{
        stale = Stale
    } = ViewArgs,
    FoldlFun = make_reduce_fold_fun(ViewArgs, Queue),
    KeyGroupFun = make_group_rows_fun(ViewArgs),
    {DDocDb, View} = get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale),
    case Keys of
    nil ->
        FoldOpts = [{key_group_fun, KeyGroupFun} |
            couch_httpd_view:make_key_options(ViewArgs)],
        {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts);
    _ when is_list(Keys) ->
        lists:foreach(
            fun(K) ->
                FoldOpts = [{key_group_fun, KeyGroupFun} |
                    couch_httpd_view:make_key_options(
                        ViewArgs#view_query_args{
                            start_key = K, end_key = K})],
                {ok, _} = couch_view:fold_reduce(View, FoldlFun, [], FoldOpts)
            end,
            Keys)
    end,
    catch couch_db:close(DDocDb).

get_reduce_view(Db, DDocDbName, DDocId, ViewName, Stale) ->
    GroupId = couch_merger:get_group_id(DDocDbName, DDocId),
    {ok, View, _} = couch_view:get_reduce_view(Db, GroupId, ViewName, Stale),
    case GroupId of
        {DDocDb, DDocId} -> {DDocDb, View};
        DDocId -> {nil, View}
    end.


make_group_rows_fun(#view_query_args{group_level = 0}) ->
    fun(_, _) -> true end;

make_group_rows_fun(#view_query_args{group_level = L}) when is_integer(L) ->
    fun({KeyA, _}, {KeyB, _}) when is_list(KeyA) andalso is_list(KeyB) ->
        lists:sublist(KeyA, L) == lists:sublist(KeyB, L);
    ({KeyA, _}, {KeyB, _}) ->
        KeyA == KeyB
    end;

make_group_rows_fun(_) ->
    fun({KeyA, _}, {KeyB, _}) -> KeyA == KeyB end.


make_reduce_fold_fun(#view_query_args{group_level = 0}, Queue) ->
    fun(_Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {null, Red}),
        {ok, Acc}
    end;

make_reduce_fold_fun(#view_query_args{group_level = L}, Queue) when is_integer(L) ->
    fun(Key, Red, Acc) when is_list(Key) ->
        ok = couch_view_merger_queue:queue(Queue, {lists:sublist(Key, L), Red}),
        {ok, Acc};
    (Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
    end;

make_reduce_fold_fun(_QueryArgs, Queue) ->
    fun(Key, Red, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, {Key, Red}),
        {ok, Acc}
    end.


get_map_view(Db, DDocDbName, DDocId, ViewName, Stale) ->
    GroupId = couch_merger:get_group_id(DDocDbName, DDocId),
    View = case couch_view:get_map_view(Db, GroupId, ViewName, Stale) of
    {ok, MapView, _} ->
        MapView;
    {not_found, _} ->
        {ok, RedView, _} = couch_view:get_reduce_view(Db, GroupId, ViewName, Stale),
        couch_view:extract_map_view(RedView)
    end,
    case GroupId of
        {DDocDb, DDocId} -> {DDocDb, View};
        DDocId -> {nil, View}
    end.


make_map_fold_fun(false, _Conflicts, _Db, Queue) ->
    fun(Row, _, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc}
    end;

make_map_fold_fun(true, Conflicts, Db, Queue) ->
    DocOpenOpts = if Conflicts -> [conflicts]; true -> [] end,
    fun({{_Key, error}, _Value} = Row, _, Acc) ->
        ok = couch_view_merger_queue:queue(Queue, Row),
        {ok, Acc};
    ({{_Key, DocId} = Kd, {Props} = Value}, _, Acc) ->
        Rev = case get_value(<<"_rev">>, Props, nil) of
        nil ->
            nil;
        Rev0 ->
            couch_doc:parse_rev(Rev0)
        end,
        IncludeId = get_value(<<"_id">>, Props, DocId),
        [Doc] = couch_httpd_view:doc_member(Db, {IncludeId, Rev}, DocOpenOpts),
        ok = couch_view_merger_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc};
    ({{_Key, DocId} = Kd, Value}, _, Acc) ->
        [Doc] = couch_httpd_view:doc_member(Db, {DocId, nil}, DocOpenOpts),
        ok = couch_view_merger_queue:queue(Queue, {Kd, Value, Doc}),
        {ok, Acc}
    end.


view_qs(ViewArgs) ->
    DefViewArgs = #view_query_args{},
    #view_query_args{
        start_key = StartKey, end_key = EndKey,
        start_docid = StartDocId, end_docid = EndDocId,
        direction = Dir,
        inclusive_end = IncEnd,
        group_level = GroupLevel,
        view_type = ViewType,
        include_docs = IncDocs,
        conflicts = Conflicts,
        stale = Stale
    } = ViewArgs,
    QsList = case StartKey =:= DefViewArgs#view_query_args.start_key of
    true ->
        [];
    false ->
        ["startkey=" ++ json_qs_val(StartKey)]
    end ++
    case EndKey =:= DefViewArgs#view_query_args.end_key of
    true ->
        [];
    false ->
        ["endkey=" ++ json_qs_val(EndKey)]
    end ++
    case {Dir, StartDocId =:= DefViewArgs#view_query_args.start_docid} of
    {fwd, false} ->
        ["startkey_docid=" ++ ?b2l(StartDocId)];
    _ ->
        []
    end ++
    case {Dir, EndDocId =:= DefViewArgs#view_query_args.end_docid} of
    {fwd, false} ->
        ["endkey_docid=" ++ ?b2l(EndDocId)];
    _ ->
        []
    end ++
    case Dir of
    fwd ->
        [];
    rev ->
        StartDocId1 = reverse_key_default(StartDocId),
        EndDocId1 = reverse_key_default(EndDocId),
        ["descending=true"] ++
        case StartDocId1 =:= DefViewArgs#view_query_args.start_docid of
        true ->
            [];
        false ->
            ["startkey_docid=" ++ json_qs_val(StartDocId1)]
        end ++
        case EndDocId1 =:= DefViewArgs#view_query_args.end_docid of
        true ->
            [];
        false ->
            ["endkey_docid=" ++ json_qs_val(EndDocId1)]
        end
    end ++
    case IncEnd =:= DefViewArgs#view_query_args.inclusive_end of
    true ->
        [];
    false ->
        ["inclusive_end=" ++ atom_to_list(IncEnd)]
    end ++
    case GroupLevel =:= DefViewArgs#view_query_args.group_level of
    true ->
        [];
    false ->
        case GroupLevel of
        exact ->
            ["group=true"];
        _ when is_number(GroupLevel) ->
            ["group_level=" ++ integer_to_list(GroupLevel)]
        end
    end ++
    case ViewType of
    red_map ->
        ["reduce=false"];
    _ ->
        []
    end ++
    case IncDocs =:= DefViewArgs#view_query_args.include_docs of
    true ->
        [];
    false ->
        ["include_docs=" ++ atom_to_list(IncDocs)]
    end ++
    case Conflicts =:= DefViewArgs#view_query_args.conflicts of
    true ->
        [];
    false ->
        ["conflicts=" ++ atom_to_list(Conflicts)]
    end ++
    case Stale =:= DefViewArgs#view_query_args.stale of
    true ->
        [];
    false ->
        ["stale=" ++ atom_to_list(Stale)]
    end,
    case QsList of
    [] ->
        [];
    _ ->
        "?" ++ string:join(QsList, "&")
    end.

json_qs_val(Value) ->
    couch_httpd:quote(?b2l(iolist_to_binary(?JSON_ENCODE(Value)))).

reverse_key_default(?MIN_STR) -> ?MAX_STR;
reverse_key_default(?MAX_STR) -> ?MIN_STR;
reverse_key_default(Key) -> Key.
