namespace java.swift org.apache.curator
namespace cpp org.apache.curator.generated
namespace java org.apache.curator.generated


enum PathChildrenCacheStartMode {
  NORMAL, BUILD_INITIAL_CACHE, POST_INITIALIZED_EVENT
}

enum CreateMode {
  PERSISTENT, PERSISTENT_SEQUENTIAL, EPHEMERAL, EPHEMERAL_SEQUENTIAL, CONTAINER
}

enum CuratorEventType {
  PING, CREATE, DELETE, EXISTS, GET_DATA, SET_DATA, CHILDREN, SYNC, GET_ACL, SET_ACL, WATCHED, CLOSING, CONNECTION_CONNECTED, CONNECTION_SUSPENDED, CONNECTION_RECONNECTED, CONNECTION_LOST, CONNECTION_READ_ONLY, LEADER, PATH_CHILDREN_CACHE, NODE_CACHE
}

enum EventType {
  None, NodeCreated, NodeDeleted, NodeDataChanged, NodeChildrenChanged
}

enum KeeperState {
  Unknown, Disconnected, NoSyncConnected, SyncConnected, AuthFailed, ConnectedReadOnly, SaslAuthenticated, Expired
}

enum PathChildrenCacheEventType {
  CHILD_ADDED, CHILD_UPDATED, CHILD_REMOVED, CONNECTION_SUSPENDED, CONNECTION_RECONNECTED, CONNECTION_LOST, INITIALIZED
}

enum PersistentEphemeralNodeMode {
  EPHEMERAL, EPHEMERAL_SEQUENTIAL, PROTECTED_EPHEMERAL, PROTECTED_EPHEMERAL_SEQUENTIAL
}

enum ExceptionType {
  GENERAL, ZOOKEEPER, NODE
}

enum NodeExceptionType {
  NONODE, BADVERSION, NODEEXISTS, NOTEMPTY
}

enum ZooKeeperExceptionType {
  SYSTEMERROR, RUNTIMEINCONSISTENCY, DATAINCONSISTENCY, CONNECTIONLOSS, MARSHALLINGERROR, UNIMPLEMENTED, OPERATIONTIMEOUT, BADARGUMENTS, APIERROR, NOAUTH, NOCHILDRENFOREPHEMERALS, INVALIDACL, AUTHFAILED, SESSIONEXPIRED, INVALIDCALLBACK, SESSIONMOVED, NOTREADONLY
}

enum DiscoveryInstanceType {
  DYNAMIC, STATIC, PERMANENT
}

enum ProviderStrategyType {
  RANDOM, STICKY_RANDOM, STICKY_ROUND_ROBIN, ROUND_ROBIN
}

struct CuratorProjection {
  1: string id;
}

struct ExistsSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
}

struct GetChildrenSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
}

struct GetDataSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
  4: bool decompressed;
}

struct LeaderEvent {
  1: string path;
  2: string participantId;
  3: bool isLeader;
}

struct LeaderProjection {
  1: string id;
}

struct LeaderResult {
  1: LeaderProjection projection;
  2: bool isLeader;
}

struct LeaseProjection {
  1: string id;
}

struct LockProjection {
  1: string id;
}

struct NodeCacheProjection {
  1: string id;
}

struct OptionalChildrenList {
  1: list<string> children;
}

struct OptionalData {
  1: binary data;
}

struct OptionalLockProjection {
  1: LockProjection lockProjection;
}

struct OptionalPath {
  1: string path;
}

struct PathChildrenCacheProjection {
  1: string id;
}

struct PersistentEphemeralNodeProjection {
  1: string id;
}

struct Id {
  1: string scheme;
  2: string id;
}

struct Participant {
  1: string id;
  2: bool isLeader;
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

struct Version {
  1: i32 version;
}

struct DiscoveryProjection {
  1: string id;
}

struct DiscoveryProviderProjection {
  1: string id;
}

struct CreateSpec {
  1: string path;
  2: binary data;
  3: CreateMode mode;
  4: string asyncContext;
  5: bool compressed;
  6: bool creatingParentsIfNeeded;
  7: bool withProtection;
  8: bool creatingParentContainersIfNeeded;
}

struct DeleteSpec {
  1: string path;
  2: bool guaranteed;
  3: string asyncContext;
  4: Version version;
}

struct OptionalStat {
  1: Stat stat;
}

struct Acl {
  1: i32 perms;
  2: Id id;
}

struct ChildData {
  1: string path;
  2: Stat stat;
  3: binary data;
}

struct PathChildrenCacheEvent {
  1: string cachedPath;
  2: PathChildrenCacheEventType type;
  3: ChildData data;
}

struct SetDataSpec {
  1: string path;
  2: bool watched;
  3: string asyncContext;
  4: bool compressed;
  5: Version version;
  6: binary data;
}

exception CuratorException {
  1: ExceptionType type;
  2: ZooKeeperExceptionType zooKeeperException;
  3: NodeExceptionType nodeException;
  4: string message;
}

struct DiscoveryInstance {
  1: string name;
  2: string id;
  3: string address;
  4: i32 port;
  5: i32 sslPort;
  6: binary payload;
  7: i64 registrationTimeUTC;
  8: DiscoveryInstanceType serviceType;
  9: string uriSpec;
}

struct CuratorEvent {
  2: CuratorEventType type;
  3: i32 resultCode;
  4: string path;
  5: string context;
  6: Stat stat;
  7: binary data;
  8: string name;
  9: list<string> children;
  10: list<Acl> aclList;
  11: WatchedEvent watchedEvent;
  12: LeaderEvent leaderEvent;
  13: PathChildrenCacheEvent childrenCacheEvent;
}

service CuratorService {
  OptionalLockProjection acquireLock(1: CuratorProjection projection, 2: string path, 3: i32 maxWaitMs) throws (1: CuratorException ex1);
  list<LeaseProjection> acquireSemaphore(1: CuratorProjection projection, 2: string path, 3: i32 acquireQty, 4: i32 maxWaitMs, 5: i32 maxLeases) throws (1: CuratorException ex1);
  void closeCuratorProjection(1: CuratorProjection projection);
  bool closeGenericProjection(1: CuratorProjection projection, 2: string id) throws (1: CuratorException ex1);
  OptionalPath createNode(1: CuratorProjection projection, 2: CreateSpec spec) throws (1: CuratorException ex1);
  void deleteNode(1: CuratorProjection projection, 2: DeleteSpec spec) throws (1: CuratorException ex1);
  OptionalStat exists(1: CuratorProjection projection, 2: ExistsSpec spec) throws (1: CuratorException ex1);
  OptionalChildrenList getChildren(1: CuratorProjection projection, 2: GetChildrenSpec spec) throws (1: CuratorException ex1);
  OptionalData getData(1: CuratorProjection projection, 2: GetDataSpec spec) throws (1: CuratorException ex1);
  list<Participant> getLeaderParticipants(1: CuratorProjection projection, 2: LeaderProjection leaderProjection) throws (1: CuratorException ex1);
  ChildData getNodeCacheData(1: CuratorProjection projection, 2: NodeCacheProjection cacheProjection) throws (1: CuratorException ex1);
  list<ChildData> getPathChildrenCacheData(1: CuratorProjection projection, 2: PathChildrenCacheProjection cacheProjection) throws (1: CuratorException ex1);
  ChildData getPathChildrenCacheDataForPath(1: CuratorProjection projection, 2: PathChildrenCacheProjection cacheProjection, 3: string path) throws (1: CuratorException ex1);
  bool isLeader(1: CuratorProjection projection, 2: LeaderProjection leaderProjection) throws (1: CuratorException ex1);
  CuratorProjection newCuratorProjection(1: string connectionName) throws (1: CuratorException ex1);
  oneway void pingCuratorProjection(1: CuratorProjection projection);
  OptionalStat setData(1: CuratorProjection projection, 2: SetDataSpec spec) throws (1: CuratorException ex1);
  LeaderResult startLeaderSelector(1: CuratorProjection projection, 2: string path, 3: string participantId, 4: i32 waitForLeadershipMs) throws (1: CuratorException ex1);
  NodeCacheProjection startNodeCache(1: CuratorProjection projection, 2: string path, 3: bool dataIsCompressed, 4: bool buildInitial) throws (1: CuratorException ex1);
  PathChildrenCacheProjection startPathChildrenCache(1: CuratorProjection projection, 2: string path, 3: bool cacheData, 4: bool dataIsCompressed, 5: PathChildrenCacheStartMode startMode) throws (1: CuratorException ex1);
  PersistentEphemeralNodeProjection startPersistentEphemeralNode(1: CuratorProjection projection, 2: string path, 3: binary data, 4: PersistentEphemeralNodeMode mode) throws (1: CuratorException ex1);
  void sync(1: CuratorProjection projection, 2: string path, 3: string asyncContext) throws (1: CuratorException ex1);
}

service EventService {
  CuratorEvent getNextEvent(1: CuratorProjection projection) throws (1: CuratorException ex1);
}

service DiscoveryService {
  list<DiscoveryInstance> getAllInstances(1: CuratorProjection projection, 2: DiscoveryProviderProjection providerProjection) throws (1: CuratorException ex1);
  DiscoveryInstance getInstance(1: CuratorProjection projection, 2: DiscoveryProviderProjection providerProjection) throws (1: CuratorException ex1);
  DiscoveryInstance makeDiscoveryInstance(1: string name, 2: binary payload, 3: i32 port) throws (1: CuratorException ex1);
  void noteError(1: CuratorProjection projection, 2: DiscoveryProviderProjection providerProjection, 3: string instanceId) throws (1: CuratorException ex1);
  DiscoveryProjection startDiscovery(1: CuratorProjection projection, 2: string basePath, 3: DiscoveryInstance yourInstance) throws (1: CuratorException ex1);
  DiscoveryProviderProjection startProvider(1: CuratorProjection projection, 2: DiscoveryProjection discoveryProjection, 3: string serviceName, 4: ProviderStrategyType providerStrategy, 5: i32 downTimeoutMs, 6: i32 downErrorThreshold) throws (1: CuratorException ex1);
}

service DiscoveryServiceLowLevel {
  DiscoveryInstance queryForInstance(1: CuratorProjection projection, 2: DiscoveryProjection discoveryProjection, 3: string name, 4: string id) throws (1: CuratorException ex1);
  list<DiscoveryInstance> queryForInstances(1: CuratorProjection projection, 2: DiscoveryProjection discoveryProjection, 3: string name) throws (1: CuratorException ex1);
  list<string> queryForNames(1: CuratorProjection projection, 2: DiscoveryProjection discoveryProjection) throws (1: CuratorException ex1);
  void registerInstance(1: CuratorProjection projection, 2: DiscoveryProjection discoveryProjection, 3: DiscoveryInstance instance) throws (1: CuratorException ex1);
  void unregisterInstance(1: CuratorProjection projection, 2: DiscoveryProjection discoveryProjection, 3: DiscoveryInstance instance) throws (1: CuratorException ex1);
  void updateInstance(1: CuratorProjection projection, 2: DiscoveryProjection discoveryProjection, 3: DiscoveryInstance instance) throws (1: CuratorException ex1);
}
