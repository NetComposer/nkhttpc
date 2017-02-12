-module(nkhttpc_test).

-compile([export_all]).


t1() ->
    Opts = #{
        % host => "cluster.netc.io", 
        host => "1.2.3.4", 
        port => 443, 
        proto => tls, 
        debug => true,
        pool_size => 3,
        auth => {basic, "user", "es"}
    },
    nkhttpc:start_link(Opts).


t2(P) ->
    Now = nklib_util:m_timestamp(),
    R = nkhttpc:req(P, get, "/es/", <<>>, #{exclusive=>true}),
    Time = nklib_util:m_timestamp() - Now,
    lager:error("Time: ~p", [Time]),
    R.
