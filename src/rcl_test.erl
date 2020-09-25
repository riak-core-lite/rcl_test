-module(rcl_test).

-export([at_init_testsuite/0, pmap/2, init_single_dc/4, start_node/4]).

init_single_dc(Suite, Config, CommonConfig, NodeConfig) ->
    ct:pal("[~p]", [Suite]),
    at_init_testsuite(),

    StartDCs =
        fun (Nodes) ->
                pmap(fun (N) ->
                             {_Status, Node} = start_node(N, Config, CommonConfig, NodeConfig),
                             Node
                     end,
                     Nodes)
        end,
    [Nodes] =
        pmap(fun (N) ->
                     StartDCs(N)
             end,
             [[dev1]]),
    [Node] = Nodes,

    [{clusters, [Nodes]}, {nodes, Nodes}, {node, Node} | Config].

at_init_testsuite() ->
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, {{already_started, _}, _}} ->
            ok
    end.

-spec pmap(fun(), list()) -> list().
pmap(F, L) ->
    Parent = self(),
    lists:foldl(fun (X, N) ->
                        spawn_link(fun () ->
                                           Parent ! {pmap, N, F(X)}
                                   end),
                        N + 1
                end,
                0,
                L),
    L2 =
        [receive
             {pmap, N, R} ->
                 {N, R}
         end
         || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

start_node(Name, Config, CommonConfig, NodeConfig) ->
    #{app := RclApp} = CommonConfig,
    #{base_port := BasePort} = NodeConfig,

    %% code path for compiled dependencies (ebin folders)
    CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()),
    ct:log("Starting node ~p", [Name]),

    {ok, Cwd} = file:get_cwd(),
    AppFolder = filename:dirname(filename:dirname(Cwd)),
    PrivDir = proplists:get_value(priv_dir, Config),
    NodeDir = filename:join([PrivDir, Name]) ++ "/",
    filelib:ensure_dir(NodeDir),

    AppDataDirName = to_ls(maps:get(data_dir_name, CommonConfig, "rcl-data")),
    BuildEnv = maps:get(build_env, CommonConfig, "dev"),

    DefaultBuildPath = io_lib:format("/_build/~s/rel/~s/lib/", [BuildEnv, RclApp]),
    BuildPath = to_ls(maps:get(build_path, CommonConfig, DefaultBuildPath)),

    DefaultSchemaDirs = io_lib:format("~s~s", [AppFolder, BuildPath]),
    SchemaDirs = to_ls(maps:get(schema_dirs, CommonConfig, DefaultSchemaDirs)),

    SetupNodeFn =
        maps:get(setup_node_fn,
                 CommonConfig,
                 fun (_) ->
                         ok
                 end),

    %% have the slave nodes monitor the runner node, so they can't outlive it
    TestNodeConfig =
        [%% have the slave nodes monitor the runner node, so they can't outlive it
         {monitor_master, true},
         %% set code path for dependencies
         {startup_functions, [{code, set_path, [CodePath]}]}],
    case ct_slave:start(Name, TestNodeConfig) of
        {ok, Node} ->
            % load application to allow for configuring the environment before starting
            ok = rpc:call(Node, application, load, [riak_core]),
            ok = rpc:call(Node, application, load, [RclApp]),

            %% get remote working dir of node
            {ok, NodeWorkingDir} = rpc:call(Node, file, get_cwd, []),

            %% DATA DIRS
            ok =
                rpc:call(Node,
                         application,
                         set_env,
                         [RclApp, data_dir, filename:join([NodeWorkingDir, Node, AppDataDirName])]),
            ok =
                rpc:call(Node,
                         application,
                         set_env,
                         [riak_core,
                          ring_state_dir,
                          filename:join([NodeWorkingDir, Node, "data"])]),
            ok =
                rpc:call(Node,
                         application,
                         set_env,
                         [riak_core,
                          platform_data_dir,
                          filename:join([NodeWorkingDir, Node, "data"])]),
            ok = rpc:call(Node, application, set_env, [riak_core, schema_dirs, [SchemaDirs]]),
            %% PORTS
            ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, BasePort]),

            SetupNodeFn(#{node => Node,
                          base_port => BasePort + 1,
                          node_dir => NodeWorkingDir,
                          node_config => TestNodeConfig,
                          common_config => CommonConfig}),

            %% LOGGING Configuration
            %% add additional logging handlers to ensure easy access to remote node logs
            %% for each logging level
            LogRoot = filename:join([NodeWorkingDir, Node, "logs"]),
            %% set the logger configuration
            ok = rpc:call(Node, application, set_env, [RclApp, logger, log_config(LogRoot)]),
            %% set primary output level, no filter
            rpc:call(Node, logger, set_primary_config, [level, all]),
            %% load additional logger handlers at remote node
            rpc:call(Node, logger, add_handlers, [RclApp]),

            %% redirect slave logs to ct_master logs
            ok = rpc:call(Node, application, set_env, [RclApp, ct_master, node()]),

            %% reduce number of actual log files created to 4, reduces start-up time of node
            ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, 4]),
            {ok, _} = rpc:call(Node, application, ensure_all_started, [RclApp]),
            ct:pal("Node ~p started with base port ~p", [Node, BasePort]),

            {connect, Node};
        {error, already_started, Node} ->
            ct:log("Node ~p already started, reusing node", [Node]),
            {ready, Node};
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            time_utils:wait_until_offline(Node),
            start_node(Name, Config, CommonConfig, NodeConfig)
    end.

%% logger configuration for each level
%% see http://erlang.org/doc/man/logger.html
log_config(LogDir) ->
    DebugConfig =
        #{level => debug,
          formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
          config => #{type => {file, filename:join(LogDir, "debug.log")}}},

    InfoConfig =
        #{level => info,
          formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
          config => #{type => {file, filename:join(LogDir, "info.log")}}},

    NoticeConfig =
        #{level => notice,
          formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
          config => #{type => {file, filename:join(LogDir, "notice.log")}}},

    WarningConfig =
        #{level => warning,
          formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
          config => #{type => {file, filename:join(LogDir, "warning.log")}}},

    ErrorConfig =
        #{level => error,
          formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
          config => #{type => {file, filename:join(LogDir, "error.log")}}},

    [{handler, debug_rcl_test, logger_std_h, DebugConfig},
     {handler, info_rcl_test, logger_std_h, InfoConfig},
     {handler, notice_rcl_test, logger_std_h, NoticeConfig},
     {handler, warning_rcl_test, logger_std_h, WarningConfig},
     {handler, error_rcl_test, logger_std_h, ErrorConfig}].

to_ls(V) when is_binary(V) ->
    binary_to_list(V);
to_ls(V) when is_list(V) ->
    lists:flatten(V).
