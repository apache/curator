namespace java.swift org.apache.curator.x.rpc.idl.event
namespace cpp org.apache.curator

include "curator.thrift"

enum CuratorEventType {
  CREATE, DELETE, EXISTS, GET_DATA, SET_DATA, CHILDREN, SYNC, GET_ACL, SET_ACL, WATCHED, CLOSING
}

enum EventType {
  None, NodeCreated, NodeDeleted, NodeDataChanged, NodeChildrenChanged
}

enum KeeperState {
  Unknown, Disconnected, NoSyncConnected, SyncConnected, AuthFailed, ConnectedReadOnly, SaslAuthenticated, Expired
}

struct id {
  1: string scheme;
  2: string id;
}

struct Stat {
  1: i64 czxid;
  2: i64 mzxid;
  3: i64 ctime;
  4: i64 mtime;
  5: i32 version;
  6: i32 cversion;
  7: i32 aversion;
  8: i64 ephemeralOwner;
  9: i32 dataLength;
  10: i32 numChildren;
  11: i64 pzxid;
}

struct WatchedEvent {
  1: KeeperState keeperState;
  2: EventType eventType;
  3: string path;
}

struct Acl {
  1: i32 perms;
  2: id id;
}

struct CuratorEvent {
  1: curator.CuratorProjection projection;
  2: CuratorEventType type;
  3: i32 resultCode;
  4: string path;
  5: string context;
  6: Stat stat;
  7: binary data;
  8: string name;
  9: list<string> children;
  10: list<Acl> aCLList;
  11: WatchedEvent watchedEvent;
}

service CuratorEventService {
  CuratorEvent getNextEvent();
}
