namespace java.swift org.apache.curator.x.rpc.idl
namespace cpp org.apache.curator



struct CuratorProjection {
  1: string id;
}

struct CuratorProjectionSpec {
}

service curator {
  void closeCuratorProjection(1: CuratorProjection projection);
  CuratorProjection newCuratorProjection(1: CuratorProjectionSpec spec);
}
