%%%-------------------------------------------------------------------
%%% File:      fat32.erl
%%% @author    Brian Buchanan <bwb@holo.org>
%%% @copyright 2008-2009 Brian Buchanan
%%% @doc  
%%% This module implements a read-only interface to a FAT12, FAT16,
%%% or FAT32 filesystem.
%%% @end  
%%%
%%%-------------------------------------------------------------------
-module(fat32).
-author('bwb@holo.org').
-behavior(gen_server).

%% API
-export([dump/1, list_directory/2, open/2, close/1, read_file/2]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

-define(SECTOR_SIZE, 512).
-define(SECTOR_CACHE_SIZE, 1000).

-type(proplist() :: [atom() | {any(), any()}]).
-type(fstype() :: fat12 | fat16 | fat32).
-type(sector() :: non_neg_integer()).
-type(cluster() :: 2..268435457).
-type(filesize() :: 0..4294967295).
-type(directory_entry_type() :: regular | directory | volume_label | invalid).

-record(mbr, {partitions}).
-record(partition_table_entry,
        {boot_flag,
         chs_begin,
         chs_end,
         type_code,
         lba_begin,
         sectors}).
-record(volume_id,
        {nfats,
         bytes_per_sector,
         total_sectors,
         sectors_per_cluster,
         reserved_sectors,
         sectors_per_fat,
         root_dir_size,
         root_cluster}).
-record(fs, 
        {fs_type::fstype(),
         fat_begin_lba,
         root_begin_lba,
         cluster_begin_lba,
         sectors_per_cluster,
         root_cluster,
         root_dir_size,
         fd,
         sector_cache}).

-include("fat32.hrl").

%%====================================================================
%% API
%%====================================================================

%% Open a FAT filesystem image.
-spec(open/2 :: (string(), proplist()) -> {ok, pid()} | ignore | {error, any()}).
open(FSPath, Options) ->
    gen_server:start_link(?MODULE, [FSPath, Options], []).

%% Close a FAT filesystem image.
-spec(close/1 :: (pid()) -> ok).
close(FS) ->
    gen_server:cast(FS, close).

%% Get the contents of a file.
-spec(read_file/2 :: (pid(), #dirent{}) -> {ok, binary()} | {error, any()}).
read_file(FS, Entry) ->
    gen_server:call(FS, {read_file, Entry}).

%% Get a directory listing.
-spec(list_directory/2 :: (pid(), #dirent{} | string()) -> {ok, [#dirent{}]} | {error, any()}).
list_directory(FS, Path) ->
    gen_server:call(FS, {list_directory, Path}).

%% For debugging purposes.
-spec(dump/1 :: (string()) -> any()).
dump(FSPath) ->
    {ok, FS} = open(FSPath, [read]),
    {ok, RootDir} = list_directory(FS, ""),
    Files = [case entry_type(Entry) of regular -> read_file(FS, Entry); directory -> list_directory(FS, Entry) end || Entry <- RootDir],
    R = {FS, RootDir, Files},
    close(FS),
    R.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([FSPath, Options]) ->
    case proplists:get_bool(write, Options) of
        true ->
            {stop, {unimplemented, write}};
        false ->
            case file:open(FSPath, [raw, binary, read]) of
                {ok, FD} ->
                    init_fs(FD);
                Error ->
                    {stop, Error}
            end
    end.

init_fs(FD) ->
    Sector0 = read_sectors(0, 1, FD),
    MBR = decode_mbr(Sector0),
    Partition1 = element(1, MBR#mbr.partitions),
    VIDSector = read_sectors(Partition1#partition_table_entry.lba_begin, 1, FD),
    VID = decode_volume_id(VIDSector),
    
    NSectors = VID#volume_id.total_sectors,
    FATStart = VID#volume_id.reserved_sectors,
    RootStart = FATStart + VID#volume_id.nfats * VID#volume_id.sectors_per_fat,
    ClustersStart = RootStart + directory_size(VID#volume_id.root_dir_size, VID#volume_id.bytes_per_sector),
    NClusters = 2 + (NSectors - ClustersStart) div VID#volume_id.sectors_per_cluster,

    %% The commonly used heuristic for determining the FAT version is to
    %% examine the number of clusters and compare against these magic values.
    FSType = if
        NClusters < 4087 ->
            fat12;
        NClusters >= 4087, NClusters < 65527 ->
            fat16;
        NClusters >= 65527, NClusters < 268435457 ->
            fat32;
        true ->
            throw({error, {bad_value, nclusters, NClusters}})
    end,
    
    BeginLBA = Partition1#partition_table_entry.lba_begin,
    {ok, #fs{fs_type=FSType,
             fat_begin_lba=BeginLBA + FATStart,
             root_begin_lba=BeginLBA + RootStart,
             cluster_begin_lba=BeginLBA + ClustersStart,
             sectors_per_cluster=VID#volume_id.sectors_per_cluster,
             root_dir_size=VID#volume_id.root_dir_size,
             root_cluster=VID#volume_id.root_cluster,
             fd=FD,
             sector_cache=lru_cache:new(?SECTOR_CACHE_SIZE)}}.

handle_call({read_file, Entry}, _From, State) ->
    {Result, State1} = do_read_file(Entry, State),
    {reply, Result, State1};
handle_call({list_directory, Path}, _From, State) ->
    {Result, State1} = do_list_directory(Path, State),
    {reply, Result, State1};
handle_call(_Msg, _From, State) ->
    {reply, {error, unrecognized}, State}.

handle_cast(close, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, FS) ->
    file:close(FS#fs.fd).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
get_root_directory(FS) ->
    {RootData, FS1} = case FS#fs.fs_type of
        fat32 ->
            do_read_file(FS#fs.root_cluster, FS);
        _ ->
            {read_sectors(FS#fs.root_begin_lba, FS#fs.root_dir_size, FS#fs.fd), FS}
    end,
    {filter_directory(decode_directory(RootData, FS#fs.fs_type)), FS1}.

do_list_directory([Char|_] = Path, FS) when is_integer(Char) ->
    do_list_directory(string:tokens(Path, "/\\"), FS);
do_list_directory(Path, FS) when is_list(Path) ->
    {RootDir, FS1} = get_root_directory(FS),
    do_list_directory(Path, RootDir, FS1);
do_list_directory(#dirent{cluster=StartCluster}, FS) ->
    {Directory, FS1} = do_read_file(StartCluster, FS),
    {{ok, filter_directory(decode_directory(Directory, FS#fs.fs_type))}, FS1}.

do_list_directory([], Entries, FS) ->
    {{ok, Entries}, FS};
do_list_directory([PathComponent|Path], Entries, FS) ->
    case find_file(string:to_upper(PathComponent), Entries) of
        {value, Entry} when Entry#dirent.attrib band ?ATTR_SUBDIR =/= 0 ->
            do_list_directory(Path, do_list_directory(Entry, FS), FS);
        {value, _Entry} ->
            {{error, enotdir}, FS};
        false ->
            {{error, enoent}, FS}
    end.

-spec(find_file/2 :: (string(), [#dirent{}]) -> {value, #dirent{}} | false).
find_file(_Name, []) ->
    false;
find_file(Name, [Entry|_]) when Entry#dirent.short_name =:= Name ->
    {value, Entry};
find_file(Name, [Entry|Entries]) ->
    case string:to_upper(Entry#dirent.name) of
        Name ->
            {value, Entry};
        _ ->
            find_file(Name, Entries)
    end.

-spec(cluster_sector/2 :: (cluster(), #fs{}) -> sector()).
cluster_sector(Cluster, FS) ->
    FS#fs.cluster_begin_lba + (Cluster - 2) * FS#fs.sectors_per_cluster.

-spec(read_cluster/2 :: (cluster(), #fs{}) -> binary()).
read_cluster(Cluster, FS) ->
    read_sectors(cluster_sector(Cluster, FS), FS#fs.sectors_per_cluster, FS#fs.fd).

-spec(read_sector/2 :: (sector(), #fs{}) -> {binary(), #fs{}}).
read_sector(N, FS) when is_record(FS, fs) ->
    case lru_cache:get(N, FS#fs.sector_cache) of
        {Sector, Cache1} ->
            {Sector, FS#fs{sector_cache=Cache1}};
        false ->
            Sector = read_sectors(N, 1, FS#fs.fd),
            {Sector, FS#fs{sector_cache=lru_cache:put(N, Sector, FS#fs.sector_cache)}}
    end.

-spec(read_sectors/3 :: (sector(), pos_integer(), any()) -> binary()).
read_sectors(Start, Count, FD) ->
    {ok, _} = file:position(FD, {bof, Start * ?SECTOR_SIZE}),
    {ok, Data} = file:read(FD, ?SECTOR_SIZE * Count),
    Data.

-spec(get_cluster_chain/2 :: (cluster(), #fs{}) -> {[cluster()], #fs{}}).
get_cluster_chain(StartCluster, FS) ->
    get_cluster_chain(StartCluster, FS, [StartCluster], sets:new()).

get_cluster_chain(Cluster, FS, Clusters, Seen) ->
    case sets:is_element(Cluster, Seen) of
        true ->
            erlang:error(cluster_chain_cycle);
        false ->
            case get_next_cluster(Cluster, FS) of
                {none, FS1} ->
                    {lists:reverse(Clusters), FS1};
                {NextCluster, FS1} ->
                    get_cluster_chain(NextCluster, FS1, [NextCluster|Clusters],
                                      sets:add_element(Cluster, Seen))
            end
    end.

-spec(read_file_clusters/2 :: (cluster(), #fs{}) -> {[binary()], #fs{}}).
read_file_clusters(StartCluster, FS) ->
    {ClusterChain, FS1} = get_cluster_chain(StartCluster, FS),
    {[read_cluster(Cluster, FS1) || Cluster <- ClusterChain], FS1}.

-spec(do_read_file/2 :: (cluster() | #dirent{}, #fs{}) -> {binary(), #fs{}}).
do_read_file(StartCluster, FS) when is_integer(StartCluster) ->
    {Clusters, FS1} = read_file_clusters(StartCluster, FS),
    {list_to_binary(Clusters), FS1};
do_read_file(#dirent{cluster=StartCluster, size=Size}, FS) ->
    read_file(StartCluster, Size, FS).

-spec(read_file/3 :: (cluster(), filesize(), #fs{}) -> {binary(), #fs{}}).
read_file(_StartCluster, 0, FS) ->
    {<<>>, FS};
read_file(StartCluster, Size, FS) ->
    Remainder = Size rem (FS#fs.sectors_per_cluster * ?SECTOR_SIZE),
    if
        Remainder =:= 0 ->
            %% File size is an exact multiple of the cluster size.
            do_read_file(StartCluster, FS);
        true ->
            %% Truncate the final cluster.
            {ClusterChain, FS1} = get_cluster_chain(StartCluster, FS),
            [LastCluster|FirstClusters] = lists:reverse(ClusterChain),
            {LastData, _} = split_binary(read_cluster(LastCluster, FS1), Remainder),
            Data = [LastData|[read_cluster(Cluster, FS1) || Cluster <- FirstClusters]],
            {list_to_binary(lists:reverse(Data)), FS1}
    end.

-spec(get_next_cluster/2 :: (cluster(), #fs{}) -> {cluster() | none, #fs{}}).
get_next_cluster(Cluster, FS = #fs{fs_type=fat32}) ->
    EntriesPerSector = ?SECTOR_SIZE div 4,
    FATSector = Cluster div EntriesPerSector,
    FATOffset = Cluster rem EntriesPerSector * 4,
    {Sector, FS1} = read_sector(FS#fs.fat_begin_lba + FATSector, FS),
    {_, <<NextCluster:32/little-unsigned-integer, _/binary>>} = split_binary(Sector, FATOffset),
    if
        NextCluster >= 16#fffffff; NextCluster =:= 0 ->
            {none, FS1};
        true ->
            {NextCluster band 16#fffffff, FS1}
    end;
get_next_cluster(Cluster, FS = #fs{fs_type=fat16}) ->
    EntriesPerSector = ?SECTOR_SIZE div 2,
    FATSector = Cluster div EntriesPerSector,
    FATOffset = Cluster rem EntriesPerSector * 2,
    {Sector, FS1} = read_sector(FS#fs.fat_begin_lba + FATSector, FS),
    {_, <<NextCluster:16/little-unsigned-integer, _/binary>>} = split_binary(Sector, FATOffset),
    if
        NextCluster >= 16#fff0; NextCluster =:= 0 ->
            {none, FS1};
        true ->
            {NextCluster, FS1}
    end;
get_next_cluster(Cluster, FS = #fs{fs_type=fat12}) ->
    %% FAT12 stores 2 entries in 3 bytes.
    Offset = Cluster div 2 * 3,
    
    {ok, _} = file:position(FS#fs.fd, ?SECTOR_SIZE * FS#fs.fat_begin_lba + Offset),
    {ok, <<AL, BL:4, AH:4, BH>>} = file:read(FS#fs.fd, 3),
    
    NextCluster = case Cluster rem 2 of
        0 ->
            AH bsl 8 bor AL;
        1 ->
            BH bsl 4 bor BL
    end,
    if
        NextCluster >= 16#ff0; NextCluster =:= 0 ->
            {none, FS};
        true ->
            {NextCluster, FS}
    end.

-spec(decode_mbr/1 :: (binary()) -> #mbr{}).
decode_mbr(Bytes) ->
    {_, PartitionTable} = split_binary(Bytes, 446),
    {Partition1, R1} = split_binary(PartitionTable, 16),
    {Partition2, R2} = split_binary(R1, 16),
    {Partition3, R3} = split_binary(R2, 16),
    {Partition4, <<16#55, 16#aa>>} = split_binary(R3, 16),
    #mbr{partitions={decode_partition_table_entry(Partition1),
                     decode_partition_table_entry(Partition2),
                     decode_partition_table_entry(Partition3),
                     decode_partition_table_entry(Partition4)}}.

-spec(decode_partition_table_entry/1 :: (binary()) -> #partition_table_entry{}).
decode_partition_table_entry(<<Boot, CHSBegin:24/little-unsigned-integer, TypeCode,
                               CHSEnd:24/little-unsigned-integer,
                               LBABegin:32/little-unsigned-integer,
                               NSectors:32/little-unsigned-integer>>) ->
    #partition_table_entry{boot_flag=Boot, chs_begin=CHSBegin, chs_end=CHSEnd,
                           type_code=TypeCode, lba_begin=LBABegin, sectors=NSectors}.

-spec(decode_volume_id/1 :: (binary()) -> #volume_id{}).
decode_volume_id(<<_:11/binary, BytesPerSector:16/little-unsigned-integer,
                   SectorsPerCluster, NReservedSectors:16/little-unsigned-integer,
                   NFATs, RootDirSize:16/little-unsigned-integer,
                   TotalSectors16:16/little-unsigned-integer, _,
                   SectorsPerFat16:16/little-unsigned-integer, _:8/binary,
                   TotalSectors32:32/little-unsigned-integer,
                   SectorsPerFAT32:32/little-unsigned-integer, _:4/binary,
                   RootCluster:32/little-unsigned-integer,
                   _:462/binary, 16#55, 16#aa>>) ->
    case BytesPerSector of
        ?SECTOR_SIZE ->
            ok;
        _ ->
            throw({error, {bad_value, bytes_per_sector, BytesPerSector}})
    end,
    if
        NFATs >= 1 ->
            ok;
        NFATs < 1 ->
            throw({error, {bad_value, nfats, NFATs}})
    end,
    #volume_id{nfats=NFATs,
               bytes_per_sector=BytesPerSector,
               total_sectors=case TotalSectors16 of 0 -> TotalSectors32; _ -> TotalSectors16 end,
               sectors_per_cluster=SectorsPerCluster,
               reserved_sectors=NReservedSectors,
               sectors_per_fat=case SectorsPerFat16 of 0 -> SectorsPerFAT32; _ -> SectorsPerFat16 end,
               root_dir_size=RootDirSize,
               root_cluster=case RootDirSize of 0 -> RootCluster; _ -> undefined end};
decode_volume_id(<<_:510/binary, Signature:16/little-unsigned-integer>>) ->
    throw({error, {bad_value, signature, Signature}}).

-spec(decode_directory/2 :: (binary(), fstype()) -> [#dirent{}]).
decode_directory(Bytes, FSType) when is_binary(Bytes) ->
    lists:reverse(decode_directory(Bytes, FSType, [])).

decode_directory(<<0, _/binary>>, _FSType, Accum) ->
    Accum;
decode_directory(<<_:1, Last:1, _:1, Seq:5,
                   Name1:10/binary, ?ATTR_LONGNAME, _, Checksum, Name2:12/binary,
                   _, _, Name3:4/binary, Rest/binary>>, FSType, Accum) ->
    decode_directory(Rest, FSType,
                     [#dirent{name={Seq, Last =:= 1, Checksum,
                                    decode_ucs2(<<Name1/binary, Name2/binary, Name3/binary>>)},
                              attrib=?ATTR_LONGNAME}|Accum]);
decode_directory(<<SName:11/binary, Attrib, _:3, LCExt:1, LCBase:1, _:3,
                   CreateFine, CreateTime:16/little-unsigned-integer,
                   CreateDate:16/little-unsigned-integer,
                   LastAccessDate:16/little-unsigned-integer,
                   ClusterHigh:16/little-unsigned-integer,
                   LastModifiedTime:16/little-unsigned-integer,
                   LastModifiedDate:16/little-unsigned-integer,
                   ClusterLow:16/little-unsigned-integer,
                   FileSize:32/little-unsigned-integer, Rest/binary>>,
                 FSType,
                 Accum) ->
    ShortName = binary_to_list(SName),
    LongName = get_longname(Accum, ShortName, LCBase, LCExt),
    Entry = #dirent{short_name=get_shortname(ShortName, 0, 0),
                    name=LongName,
                    attrib=Attrib,
                    cluster=case FSType of fat32 -> ClusterHigh bsl 16 bor ClusterLow; _ -> ClusterLow end,
                    created={decode_date(CreateDate), decode_time(CreateTime, CreateFine)},
                    accessed={decode_date(LastAccessDate), {0, 0, 0}},
                    modified={decode_date(LastModifiedDate),
                              decode_time(LastModifiedTime, 0)},
                    size=FileSize},
    decode_directory(Rest, FSType, [Entry|Accum]);
decode_directory(<<>>, _FSType, Accum) ->
    Accum.

-spec(get_longname/4 :: ([#dirent{}], string(), 0 | 1, 0| 1) -> string()).
get_longname(Entries, ShortName, LCBase, LCExt) ->
    get_longname(Entries, get_shortname(ShortName, LCBase, LCExt),
                 "", 1, checksum_shortname(ShortName)).

get_longname([#dirent{attrib=?ATTR_LONGNAME, name={Seq, Last, Checksum, Name}}|Entries],
             ShortName, Accum, Seq, Checksum) ->
    case Last of
        true ->
            lists:append(lists:reverse([Name|Accum]));
        false ->
            get_longname(Entries, ShortName, [Name|Accum],
                         Seq + 1, Checksum)
        end;
get_longname(_, ShortName, _, _, _) ->
    ShortName.

-spec(get_shortname/3 :: (string(), 0 | 1, 0 | 1) -> string()).
get_shortname(ShortName, LCBase, LCExt) ->
    {Base, Ext} = get_shortname1(ShortName),
    Base1 = if LCBase =:= 1 -> string:to_lower(Base); true -> Base end,
    case Ext of
        "" ->
            Base1;
        _ ->
            Ext1 = if LCExt =:= 1 -> string:to_lower(Ext); true -> Ext end,
            lists:append([Base1, ".", Ext1])
    end.

get_shortname1([_, _, _, _, _, _, _, _, 32, 32, 32] = ShortName) ->
    {string:strip(ShortName, right, 32), ""};
get_shortname1(ShortName) when length(ShortName) =:= 11 ->
    {string:strip(string:substr(ShortName, 1, 8), right, 32),
     string:strip(string:substr(ShortName, 9), right, 32)}.

-spec(checksum_shortname/1 :: (string()) -> 0..255).
checksum_shortname(ShortName) ->
    checksum_shortname(ShortName, 0).

checksum_shortname([C|Rest], Sum) ->
    checksum_shortname(Rest, ((Sum bsr 1) + ((Sum band 1) bsl 7) + C) band 16#ff);
checksum_shortname([], Sum) ->
    Sum.

-spec(filter_directory/1 :: ([#dirent{}]) -> [#dirent{}]).
filter_directory(Entries) ->
    [Entry || Entry <- Entries, is_live_entry(Entry)].

-spec(is_live_entry/1 :: (#dirent{}) -> bool()).
is_live_entry(#dirent{short_name=[16#e5|_]}) ->
    %% Deleted file.
    false;
is_live_entry(Entry) ->
    case entry_type(Entry) of
        directory ->
            true;
        regular ->
            true;
        _ ->
            false
    end.

-spec(entry_type/1 :: (#dirent{}) -> directory_entry_type()).
entry_type(#dirent{attrib=Attrib}) when Attrib band ?ATTR_SUBDIR =:= ?ATTR_SUBDIR ->
    directory;
entry_type(#dirent{attrib=Attrib}) when Attrib band 16#18 =:= 0 ->
    regular;
entry_type(#dirent{attrib=Attrib}) when Attrib band ?ATTR_VOLLBL =:= ?ATTR_VOLLBL ->
    volume_label;
entry_type(_) ->
    invalid.

-spec(decode_date/1 :: (integer()) -> {pos_integer(), non_neg_integer(), non_neg_integer()}).
decode_date(Value) ->
    {1980 + (Value bsr 9), (Value bsr 5) band 16#0f, Value band 16#1f}.

-spec(decode_time/2 :: (integer(), integer()) -> {non_neg_integer(), non_neg_integer(), float()}).
decode_time(TVal, TFine) ->
    {TVal bsr 11, (TVal bsr 5) band 16#3f, (TVal band 16#1f) + TFine / 100}.

-spec(decode_ucs2/1 :: (binary()) -> string()).
decode_ucs2(Binary) ->
    lists:reverse(decode_ucs2(Binary, [])).

decode_ucs2(<<0, 0, _/binary>>, Accum) ->
    Accum;
decode_ucs2(<<Char:16/little-unsigned-integer, Rest/binary>>, Accum) ->
    decode_ucs2(Rest, [Char|Accum]);
decode_ucs2(<<>>, Accum) ->
    Accum.

-spec(directory_size/2 :: (non_neg_integer(), non_neg_integer()) -> non_neg_integer()).
directory_size(NEntries, SectorSize) ->
    Sectors = NEntries * 32 div SectorSize,
    case NEntries rem SectorSize of
        0 ->
            Sectors;
        _ ->
            Sectors + 1
    end.
